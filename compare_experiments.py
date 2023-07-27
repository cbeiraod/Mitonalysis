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

from process_all_assays import script_main as process_all_assays

def compare_individual_assays_task(
                        Zacarias: RM.RunManager,
                        logger: logging.Logger,
                        marginal_type: str = "rug",
                        task_name: str = "compare_assays",
                        ):
    if not Zacarias.task_completed("join_experiments"):
        raise RuntimeError("Only call the plotter task after the joiner task has successfully completed")

    with open(Zacarias.data_directory/"all_measurements.pkl", 'rb') as pickle_file:
        all_measurements = pickle.load(pickle_file)

    full_df = pandas.read_csv(Zacarias.data_directory/"all_data.csv")
    runs = sorted(full_df["Run Number"].unique())

    with Zacarias.handle_task(task_name, drop_old_data=True, loop_iterations = len(runs)) as Rembrandt:
        for run in runs:
            output_dir = Rembrandt.task_path / f'assay_{run}'
            output_dir.mkdir(exist_ok = True)

            run_df : pandas.DataFrame = full_df.loc[full_df["Run Number"] == run]

            # Get sliced df with a single value for each of the summary values
            run_df.set_index(["Run Type", "Mitochondria"], inplace=True)
            sliced_df = run_df[~run_df.index.duplicated(keep='last')]
            sliced_df.reset_index(inplace=True)
            run_df.reset_index(inplace=True)

            for measurement in all_measurements:
                utilities.make_histogram_plot(
                    data_df = sliced_df,
                    x_var = f'{measurement} Mean',
                    base_path = output_dir,
                    file_name = f'{measurement}_mean',
                    run_name = f'{Rembrandt.run_name} - Assay {run}',
                    nbins = 100,
                    logy = True,
                    x_label = utilities.measurement_to_label(measurement),
                    marginal_type = marginal_type,
                    group_var = "Run Type",
                )

                utilities.make_histogram_plot(
                    data_df = sliced_df,
                    x_var = f'{measurement} Median',
                    base_path = output_dir,
                    file_name = f'{measurement}_median',
                    run_name = f'{Rembrandt.run_name} - Assay {run}',
                    nbins = 100,
                    logy = True,
                    x_label = utilities.measurement_to_label(measurement),
                    marginal_type = marginal_type,
                    group_var = "Run Type",
                )

                utilities.make_histogram_plot(
                    data_df = sliced_df,
                    x_var = f'{measurement} Standard Deviation',
                    base_path = output_dir,
                    file_name = f'{measurement}_std',
                    run_name = f'{Rembrandt.run_name} - Assay {run}',
                    nbins = 100,
                    logy = True,
                    x_label = utilities.measurement_to_label(measurement),
                    marginal_type = marginal_type,
                    group_var = "Run Type",
                )

                utilities.make_histogram_plot(
                    data_df = run_df,
                    x_var = f'{measurement}',
                    base_path = output_dir,
                    file_name = f'{measurement}',
                    run_name = f'{Rembrandt.run_name} - Assay {run}',
                    nbins = 100,
                    logy = True,
                    marginal_type = marginal_type,
                    x_label = utilities.measurement_to_label(measurement),
                    group_var = "Run Type",
                )

                utilities.make_histogram_plot(
                    data_df = run_df,
                    x_var = f'{measurement}',
                    base_path = output_dir,
                    file_name = f'{measurement}_movementTag',
                    run_name = f'{Rembrandt.run_name} - Assay {run}',
                    nbins = 100,
                    logy = True,
                    marginal_type = marginal_type,
                    x_label = utilities.measurement_to_label(measurement),
                    group_var = "Run Type",
                    pattern_shape_var = "Has Moved",
                )

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
                run_name = f'{Rembrandt.run_name} - Assay {run}',
                base_path = output_dir,
                dimensions = mean_measurements,
                labels = mean_labels,
                file_name = "multi_scatter_mean",
                color_var = "Run Type",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = sliced_df,
                run_name = f'{Rembrandt.run_name} - Assay {run}',
                base_path = output_dir,
                dimensions = median_measurements,
                labels = median_labels,
                file_name = "multi_scatter_median",
                color_var = "Run Type",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = sliced_df,
                run_name = f'{Rembrandt.run_name} - Assay {run}',
                base_path = output_dir,
                dimensions = std_measurements,
                labels = std_labels,
                file_name = "multi_scatter_std",
                color_var = "Run Type",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = run_df,
                run_name = f'{Rembrandt.run_name} - Assay {run}',
                base_path = output_dir,
                dimensions = all_measurements,
                labels = labels,
                file_name = "multi_scatter",
                color_var = "Run Type",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = run_df,
                run_name = f'{Rembrandt.run_name} - Assay {run}',
                base_path = output_dir,
                dimensions = all_measurements,
                labels = labels,
                file_name = "multi_scatter_movementTag",
                color_var = "Run Type",
                symbol_var = "Has Moved",
                opacity = 0.5,
            )

            Rembrandt.loop_tick()


def summarise_experiments_task(
                        Zacarias: RM.RunManager,
                        logger: logging.Logger,
                        marginal_type: str = "rug",
                        task_name: str = "plot_summary",
                        ):
    if not Zacarias.task_completed("join_experiments"):
        raise RuntimeError("Only call the plotter task after the joiner task has successfully completed")

    with open(Zacarias.data_directory/"all_measurements.pkl", 'rb') as pickle_file:
        all_measurements = pickle.load(pickle_file)

    with Zacarias.handle_task(task_name, drop_old_data=True, loop_iterations = len(all_measurements)) as Picasso:
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
                group_var = "Run Type",
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
                group_var = "Run Type",
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
                group_var = "Run Type",
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
                group_var = "Run Type",
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
                group_var = "Run Type",
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
            color_var = "Run Type",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = sliced_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = median_measurements,
            labels = median_labels,
            file_name = "multi_scatter_median",
            color_var = "Run Type",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = sliced_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = std_measurements,
            labels = std_labels,
            file_name = "multi_scatter_std",
            color_var = "Run Type",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = full_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = all_measurements,
            labels = labels,
            file_name = "multi_scatter",
            color_var = "Run Type",
            opacity = 0.5,
        )
        utilities.make_multiscatter_plot(
            data_df = full_df,
            run_name = Picasso.run_name,
            base_path = Picasso.task_path,
            dimensions = all_measurements,
            labels = labels,
            file_name = "multi_scatter_movementTag",
            color_var = "Run Type",
            symbol_var = "Has Moved",
            opacity = 0.5,
        )

def join_experiment_data(
                    Zacarias: RM.RunManager,
                    experiment_list: list[str],
                    logger: logging.Logger,
                    ):
    if not Zacarias.task_completed("read_experiments"):
        raise RuntimeError("Only call the joiner task after the read experiments task has successfully completed")

    with Zacarias.handle_task("join_experiments", drop_old_data=True, loop_iterations = len(experiment_list)) as Martin:
        merged_df = None
        merged_measurements = None

        for experiment in experiment_list:
            experiment_run_dir = Martin.path_directory.parent / experiment
            Bob = RM.RunManager(experiment_run_dir)
            if not Bob.task_completed("join_assays"):
                logger.error(f"The join assays task has not completed for experiment {experiment}")
                continue

            experiment_df = pandas.read_csv(Bob.data_directory / "all_data.csv")

            with open(Bob.data_directory/"all_measurements.pkl", 'rb') as pickle_file:
                experiment_measurements = pickle.load(pickle_file)

            if merged_df is None:
                merged_df = experiment_df
                merged_measurements = experiment_measurements
            else:
                merged_df = pandas.concat([merged_df, experiment_df])
                previous_measurements = merged_measurements
                merged_measurements = []
                for measurement in previous_measurements:
                    if measurement in experiment_measurements:
                        merged_measurements += [measurement]

            Martin.loop_tick()

        merged_df.set_index(["Run Number","Run Type","Mitochondria","Measurement"], inplace = True)
        merged_df.sort_index(inplace=True)

        if not Martin.data_directory.exists():
            Martin.data_directory.mkdir()
        merged_df.to_csv(Martin.data_directory/"all_data.csv")

        with open(Martin.data_directory/"all_measurements.pkl", 'wb') as pickle_file:
            pickle.dump(merged_measurements, pickle_file)

def read_experiments_task(
                    Zacarias: RM.RunManager,
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

    with Zacarias.handle_task("read_experiments", drop_old_data=True, loop_iterations = len(dir_list)) as Harry:
        run_list = []
        for dir_path in dir_list:
            process_all_assays(
                mitometer_path = dir_path,
                run_name = f'processed_{dir_path.name}',
                output_path = Zacarias.path_directory.parent,
                marginal_type = marginal_type,
                disable_plots = disable_plots,
            )

            run_list += [f'processed_{dir_path.name}']
            Harry.loop_tick()

        run_list.sort()

        return run_list

def script_main(
                mitometer_path: Path,
                run_name: str,
                output_path: Path,
                marginal_type: str = "rug",
                disable_plots: bool = False,
                compare_individual: bool = False,
                ):
    logger = logging.getLogger('compare_experiments')

    with RM.RunManager(output_path / run_name) as Zacarias:
        Zacarias.create_run(raise_error=False)

        run_list = read_experiments_task(
                         Zacarias = Zacarias,
                         mitometer_path = mitometer_path,
                         logger = logger,
                         marginal_type = marginal_type,
                         disable_plots = disable_plots,
                         )

        join_experiment_data(
            Zacarias = Zacarias,
            experiment_list = run_list,
            logger = logger
        )

        if not disable_plots:
            summarise_experiments_task(
                Zacarias = Zacarias,
                logger = logger,
                marginal_type = marginal_type,
            )

        if compare_individual:
            compare_individual_assays_task(
                Zacarias = Zacarias,
                logger = logger,
                marginal_type = marginal_type,
            )
            pass

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
                    prog='compare_experiments.py',
                    description='This script reads the translated mitometer files in txt format for several experiments',
                    #epilog='Text at the bottom of help'
                    )

    parser.add_argument(
        '-m',
        '--mitometer',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the directory containing the subdirectories with the translated mitometer files in txt format for each experiment',
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
        '-c',
        '--compareIndividual',
        help = 'If set, plots comparing the individual assays of the several experiments will be produced',
        action = 'store_true',
        dest = 'compare_individual',
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

    script_main(mitometer_path, args.run_name, output_path, marginal_type, args.disable_plots, args.compare_individual)