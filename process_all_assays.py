#############################################################################
# zlib License
#
# (C) 2023 Cristóvão Beirão da Cruz e Silva <cbeiraod@cern.ch>
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.
#############################################################################

from pathlib import Path
import logging
import pandas
import pickle

import lip_pps_run_manager as RM

import utilities

from read_mitometer_file import script_main as read_mitometer_file

def summarise_all_assays_task(
                        Leonardo: RM.RunManager,
                        logger: logging.Logger,
                        marginal_type: str = "rug",
                        task_name: str = "plot_summary",
                        ):
    if not Leonardo.task_completed("join_assays"):
        raise RuntimeError("Only call the plotter task after the joiner task has successfully completed")

    with open(Leonardo.data_directory/"all_measurements.pkl", 'rb') as pickle_file:
        all_measurements = pickle.load(pickle_file)

    with Leonardo.handle_task(task_name, drop_old_data=True, loop_iterations = len(all_measurements)) as Picasso:
        full_df = pandas.read_csv(Picasso.data_directory/"all_data.csv")

        # Get sliced df with a single value for each of the summary values
        full_df.set_index(["Run ID", "Mitochondria"], inplace=True)
        sliced_df = full_df[~full_df.index.duplicated(keep='last')]
        sliced_df.reset_index(inplace=True)
        full_df.reset_index(inplace=True)

        for measurement in all_measurements:
            utilities.make_histogram_plot(
                data_df = sliced_df,
                x_var = f'{measurement} Mean',
                base_path = Picasso.task_path,
                file_name = f'{measurement}_mean',
                run_name = Picasso.run_name,
                nbins = 100,
                logy = True,
                x_label = utilities.measurement_to_label(measurement),
                marginal_type = marginal_type,
            )

            utilities.make_histogram_plot(
                data_df = sliced_df,
                x_var = f'{measurement} Median',
                base_path = Picasso.task_path,
                file_name = f'{measurement}_median',
                run_name = Picasso.run_name,
                nbins = 100,
                logy = True,
                x_label = utilities.measurement_to_label(measurement),
                marginal_type = marginal_type,
            )

            utilities.make_histogram_plot(
                data_df = sliced_df,
                x_var = f'{measurement} Standard Deviation',
                base_path = Picasso.task_path,
                file_name = f'{measurement}_std',
                run_name = Picasso.run_name,
                nbins = 100,
                logy = True,
                x_label = utilities.measurement_to_label(measurement),
                marginal_type = marginal_type,
            )

            utilities.make_histogram_plot(
                data_df = full_df,
                x_var = f'{measurement}',
                base_path = Picasso.task_path,
                file_name = f'{measurement}',
                run_name = Picasso.run_name,
                nbins = 100,
                logy = True,
                marginal_type = marginal_type,
                x_label = utilities.measurement_to_label(measurement),
            )

            utilities.make_histogram_plot(
                data_df = full_df,
                x_var = f'{measurement}',
                base_path = Picasso.task_path,
                file_name = f'{measurement}_movementTag',
                run_name = Picasso.run_name,
                nbins = 100,
                logy = True,
                marginal_type = marginal_type,
                x_label = utilities.measurement_to_label(measurement),
                group_var = "Has Moved",
            )

            utilities.make_histogram_plot(
                data_df = full_df,
                x_var = f'{measurement}',
                base_path = Picasso.task_path,
                file_name = f'{measurement}_runTag',
                run_name = Picasso.run_name,
                nbins = 100,
                logy = True,
                marginal_type = None,
                x_label = utilities.measurement_to_label(measurement),
                group_var = "Run ID",
            )

            utilities.make_histogram_plot(
                data_df = full_df,
                x_var = f'{measurement}',
                base_path = Picasso.task_path,
                file_name = f'{measurement}_movementRunTag',
                run_name = Picasso.run_name,
                nbins = 100,
                logy = True,
                marginal_type = None,
                x_label = utilities.measurement_to_label(measurement),
                group_var = "Run ID",
                pattern_shape_var = "Has Moved",
            )

            if marginal_type == "box":
                utilities.make_box_plot(
                    data_df = full_df,
                    x_var = f'{measurement}',
                    base_path = Picasso.task_path,
                    file_name = f'{measurement}_runTag',
                    run_name = Picasso.run_name,
                    x_label = utilities.measurement_to_label(measurement),
                    group_var = "Run ID",
                )

                utilities.make_box_plot(
                    data_df = full_df,
                    x_var = f'{measurement}',
                    base_path = Picasso.task_path,
                    file_name = f'{measurement}_movementRunTag',
                    run_name = Picasso.run_name,
                    x_label = utilities.measurement_to_label(measurement),
                    group_var = "Run ID",
                    pattern_shape_var = "Has Moved",
                )
            elif marginal_type == "violin":
                utilities.make_violin_plot(
                    data_df = full_df,
                    x_var = f'{measurement}',
                    base_path = Picasso.task_path,
                    file_name = f'{measurement}_runTag',
                    run_name = Picasso.run_name,
                    x_label = utilities.measurement_to_label(measurement),
                    group_var = "Run ID",
                )

                utilities.make_violin_plot(
                    data_df = full_df,
                    x_var = f'{measurement}',
                    base_path = Picasso.task_path,
                    file_name = f'{measurement}_movementRunTag',
                    run_name = Picasso.run_name,
                    x_label = utilities.measurement_to_label(measurement),
                    group_var = "Run ID",
                    pattern_shape_var = "Has Moved",
                )

            Picasso.loop_tick()

        mean_measurements = []
        median_measurements = []
        std_measurements = []
        mean_labels = {}
        median_labels = {}
        std_labels = {}
        labels = {}
        for measurement in all_measurements:
            mean_measurement   = f'{measurement} Mean'
            median_measurement = f'{measurement} Median'
            std_measurement    = f'{measurement} Standard Deviation'

            mean_measurements   += [mean_measurement]
            median_measurements += [median_measurement]
            std_measurements    += [std_measurement]

            label = utilities.measurement_to_label(measurement)
            labels[measurement] = label
            mean_labels[mean_measurement]     = label
            median_labels[median_measurement] = label
            std_labels[std_measurement]       = label

        utilities.make_multiscatter_plot(
            data_df = sliced_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = mean_measurements,
            labels = mean_labels,
            file_name = "multi_scatter_mean",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = sliced_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = median_measurements,
            labels = median_labels,
            file_name = "multi_scatter_median",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = sliced_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = std_measurements,
            labels = std_labels,
            file_name = "multi_scatter_std",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = full_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = all_measurements,
            labels = labels,
            file_name = "multi_scatter",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = full_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = all_measurements,
            labels = labels,
            file_name = "multi_scatter_movementTag",
            color_var = "Has Moved",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = full_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = all_measurements,
            labels = labels,
            file_name = "multi_scatter_runTag",
            color_var = "Run ID",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = full_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = all_measurements,
            labels = labels,
            file_name = "multi_scatter_movementRunTag",
            color_var = "Run ID",
            symbol_var = "Has Moved",
            opacity = 0.5,
        )

def join_assay_data(
                    Leonardo: RM.RunManager,
                    assay_list: list[str],
                    logger: logging.Logger,
                    ):
    if not Leonardo.task_completed("read_all_assays"):
        pass
        raise RuntimeError("Only call the joiner task after the read all assays task has successfully completed")

    with Leonardo.handle_task("join_assays", drop_old_data=True, loop_iterations = len(assay_list)) as Gustavo:
        merged_df = None
        merged_measurements = None

        for assay in assay_list:
            assay_run_dir = Leonardo.path_directory.parent / assay
            Bob = RM.RunManager(assay_run_dir)
            if not Bob.task_completed("read_mitometer"):
                logger.error(f"The read mitometer task has not completed for run {assay}")
                continue

            assay_df = pandas.read_csv(Bob.data_directory / "all_data.csv")

            with open(Bob.data_directory/"all_measurements.pkl", 'rb') as pickle_file:
                assay_measurements = pickle.load(pickle_file)

            if merged_df is None:
                merged_df = assay_df
                merged_measurements = assay_measurements
            else:
                merged_df = pandas.concat([merged_df, assay_df])
                previous_measurements = merged_measurements
                merged_measurements = []
                for measurement in previous_measurements:
                    if measurement in assay_measurements:
                        merged_measurements += [measurement]

            Gustavo.loop_tick()

        merged_df.set_index(["Run Number","Mitochondria", "Measurement"], inplace = True)
        merged_df.sort_index(inplace=True)

        if not Gustavo.data_directory.exists():
            Gustavo.data_directory.mkdir()
        merged_df.to_csv(Gustavo.data_directory/"all_data.csv")

        with open(Gustavo.data_directory/"all_measurements.pkl", 'wb') as pickle_file:
            pickle.dump(merged_measurements, pickle_file)

def read_assays_task(
                    Leonardo: RM.RunManager,
                    mitometer_path: Path,
                    logger: logging.Logger,
                    marginal_type: str = "rug",
                    disable_plots: bool = False,
                    ):
    dir_list = []
    for dir_path in mitometer_path.iterdir():
        if dir_path.is_dir():
            dir_list += [dir_path]
    del dir_path

    with Leonardo.handle_task("read_all_assays", drop_old_data=True, loop_iterations = len(dir_list)) as Matt:
        run_list = []
        for dir_path in dir_list:
            read_mitometer_file(
                mitometer_path = dir_path,
                run_name = mitometer_path.name + "_" + dir_path.name,
                output_path = Leonardo.path_directory.parent,
                marginal_type = marginal_type,
                disable_plots = disable_plots,
            )

            run_list += [mitometer_path.name + "_" + dir_path.name]
            #if Matt.processed_iterations == 13:
            #    break
            Matt.loop_tick()

        return run_list

def script_main(
                mitometer_path: Path,
                run_name: str,
                output_path: Path,
                marginal_type: str = "rug",
                disable_plots: bool = False,
                ):
    logger = logging.getLogger('process_all_assays')

    with RM.RunManager(output_path / run_name) as Leonardo:
        Leonardo.create_run(raise_error=False)

        run_list = read_assays_task(
                         Leonardo = Leonardo,
                         mitometer_path = mitometer_path,
                         logger = logger,
                         marginal_type = marginal_type,
                         disable_plots = disable_plots,
                         )

        join_assay_data(
            Leonardo = Leonardo,
            assay_list = run_list,
            logger = logger
        )

        if not disable_plots:
            summarise_all_assays_task(
                Leonardo = Leonardo,
                logger = logger,
                marginal_type = marginal_type,
            )

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
                    prog='process_all_assays.py',
                    description='This script reads the translated mitometer files in txt format for several assays',
                    #epilog='Text at the bottom of help'
                    )

    parser.add_argument(
        '-m',
        '--mitometer',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the directory containing the subdirectories with the translated mitometer files in txt format',
        required = True,
        dest = 'mitometer_path',
    )
    parser.add_argument(
        '-n',
        '--runName',
        metavar = 'NAME',
        type = str,
        help = 'Name to give to this run. The name will be used to create a directory where the contents will be stored',
        required = True,
        dest = 'run_name',
    )
    parser.add_argument(
        '-o',
        '--outputPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the output directory where to place the comparison run output and where the run directories can be found',
        required = True,
        dest = 'output_path',
    )
    parser.add_argument(
        '--marginalType',
        metavar = 'TYPE',
        type = str,
        help = 'Set the type of marginal distribution in the plots. Default: box',
        choices = ["None","rug","box","violin"],
        default = "box",
        dest = 'marginal_type',
    )
    parser.add_argument(
        '-d',
        '--disablePlots',
        help = 'If set, it disables the generation of the summary plots',
        action = 'store_true',
        dest = 'disable_plots',
    )
    parser.add_argument(
        '-l',
        '--log-level',
        metavar = 'LEVEL',
        type = str,
        help = 'Set the logging level. Default: WARNING',
        choices = ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"],
        default = "WARNING",
        dest = 'log_level',
    )
    parser.add_argument(
        '--log-file',
        help = 'If set, the full log will be saved to a file (i.e. the log level is ignored)',
        action = 'store_true',
        dest = 'log_file',
    )

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(filename='logging.log', filemode='w', encoding='utf-8', level=logging.NOTSET)
    else:
        if args.log_level == "CRITICAL":
            logging.basicConfig(level=50)
        elif args.log_level == "ERROR":
            logging.basicConfig(level=40)
        elif args.log_level == "WARNING":
            logging.basicConfig(level=30)
        elif args.log_level == "INFO":
            logging.basicConfig(level=20)
        elif args.log_level == "DEBUG":
            logging.basicConfig(level=10)
        elif args.log_level == "NOTSET":
            logging.basicConfig(level=0)

    mitometer_path: Path = args.mitometer_path
    if not mitometer_path.exists() or not mitometer_path.is_dir():
        logging.error("You must define a valid mitometer data path")
        exit(1)
    mitometer_path = mitometer_path.absolute()

    output_path: Path = args.output_path
    if not output_path.exists() or not output_path.is_dir():
        logging.error("You must define a valid data output path")
        exit(1)
    output_path = output_path.absolute()

    marginal_type: str = args.marginal_type
    if marginal_type == "None":
        marginal_type = None

    script_main(mitometer_path, args.run_name, output_path, marginal_type, args.disable_plots)