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

def plot_summary_task(
                        Tiago: RM.RunManager,
                        logger: logging.Logger,
                        marginal_type: str = "rug",
                        task_name: str = "plot_summary",
                        ):
    if not Tiago.task_completed("read_mitometer"):
        raise RuntimeError("Only call the plotter task after the read mitometer task has successfully completed")
    else:
        with open(Tiago.data_directory/"all_measurements.pkl", 'rb') as pickle_file:
            all_measurements = pickle.load(pickle_file)

        with Tiago.handle_task(task_name, drop_old_data=True, loop_iterations = len(all_measurements)) as Monet:
            run_df = pandas.read_csv(Tiago.data_directory/"all_data.csv")

            # Get sliced df with a single value for each of the summary values
            run_df.set_index("Mitochondria", inplace=True)
            sliced_df = run_df[~run_df.index.duplicated(keep='last')]
            sliced_df.reset_index(inplace=True)
            run_df.reset_index(inplace=True)

            # Create category for no movement
            run_df["Has Moved"] = (~(run_df["displacement"] == 0) * 1)
            run_df["Has Moved"] = (~(run_df["displacement"] == 0))

            for measurement in all_measurements:
                # measurement_df = run_df.pivot(index=["Mitochondria"], columns="Measurement", values=measurement)

                utilities.make_histogram_plot(
                    data_df = sliced_df,
                    x_var = f'{measurement} Mean',
                    base_path = Monet.task_path,
                    file_name = f'{measurement}_mean',
                    run_name = Monet.run_name,
                    nbins = 100,
                    logy = True,
                    x_label = utilities.measurement_to_label(measurement),
                    marginal_type = marginal_type,
                )

                utilities.make_histogram_plot(
                    data_df = sliced_df,
                    x_var = f'{measurement} Median',
                    base_path = Monet.task_path,
                    file_name = f'{measurement}_median',
                    run_name = Monet.run_name,
                    nbins = 100,
                    logy = True,
                    x_label = utilities.measurement_to_label(measurement),
                    marginal_type = marginal_type,
                )

                utilities.make_histogram_plot(
                    data_df = sliced_df,
                    x_var = f'{measurement} Standard Deviation',
                    base_path = Monet.task_path,
                    file_name = f'{measurement}_std',
                    run_name = Monet.run_name,
                    nbins = 100,
                    logy = True,
                    x_label = utilities.measurement_to_label(measurement),
                    marginal_type = marginal_type,
                )

                utilities.make_histogram_plot(
                    data_df = run_df,
                    x_var = f'{measurement}',
                    base_path = Monet.task_path,
                    file_name = f'{measurement}',
                    run_name = Monet.run_name,
                    nbins = 100,
                    logy = True,
                    marginal_type = marginal_type,
                    x_label = utilities.measurement_to_label(measurement),
                )

                utilities.make_histogram_plot(
                    data_df = run_df,
                    x_var = f'{measurement}',
                    base_path = Monet.task_path,
                    file_name = f'{measurement}_movementTag',
                    run_name = Monet.run_name,
                    nbins = 100,
                    logy = True,
                    marginal_type = marginal_type,
                    x_label = utilities.measurement_to_label(measurement),
                    group_var = "Has Moved",
                )

                Monet.loop_tick()

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
                run_name = Monet.run_name,
                base_path = Monet.task_path,
                dimensions = mean_measurements,
                labels = mean_labels,
                file_name = "multi_scatter_mean",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = sliced_df,
                run_name = Monet.run_name,
                base_path = Monet.task_path,
                dimensions = median_measurements,
                labels = median_labels,
                file_name = "multi_scatter_median",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = sliced_df,
                run_name = Monet.run_name,
                base_path = Monet.task_path,
                dimensions = std_measurements,
                labels = std_labels,
                file_name = "multi_scatter_std",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = run_df,
                run_name = Monet.run_name,
                base_path = Monet.task_path,
                dimensions = all_measurements,
                labels = labels,
                file_name = "multi_scatter",
                opacity = 0.5,
            )
            utilities.make_multiscatter_plot(
                data_df = run_df,
                run_name = Monet.run_name,
                base_path = Monet.task_path,
                dimensions = all_measurements,
                labels = labels,
                file_name = "multi_scatter_movementTag",
                color_var = "Has Moved",
                opacity = 0.5,
            )

def read_mitometer_task(
                        Tiago: RM.RunManager,
                        mitometer_path: Path,
                        logger: logging.Logger,
                        ):
    file_list = utilities.get_sorted_measurements_from_path(mitometer_path, logger, first_measurements = ["distance", "displacement"])

    with Tiago.handle_task("read_mitometer", drop_old_data=True, loop_iterations = len(file_list)) as Joana:
        run_df = None
        all_measurements = []
        for file in file_list:
            # Get Measurement name
            file_name: str = file.name
            measurement_index = file_name.find(".tif_")
            if measurement_index >= 0:
                measurement_name = file_name[measurement_index+5:-4]
            else:
                measurement_name = file_name

            # Skip measurement types we do not care about
            if measurement_name in ["fission", "fusion"]:
                Joana.loop_tick()
                continue
            all_measurements += [measurement_name]

            # Get base dataframes
            file_df = pandas.read_csv(file, header=None)
            summary_df = pandas.DataFrame()

            # Get measurements
            measurements = file_df.columns

            # Get means/medians
            summary_df[f'{measurement_name} Mean'] = file_df[measurements].mean(axis = 1)
            summary_df[f'{measurement_name} Standard Deviation'] = file_df[measurements].std(axis = 1)
            summary_df[f'{measurement_name} Median'] = file_df[measurements].median(axis = 1)

            # Create Mitochondria index
            file_df = file_df.reset_index()
            file_df.rename(columns={"index": "Mitochondria"}, inplace=True)
            summary_df = summary_df.reset_index()
            summary_df.rename(columns={"index": "Mitochondria"}, inplace=True)
            summary_df.set_index("Mitochondria", inplace = True)

            # Reorganise data into rows for each measurement
            file_df = file_df.melt(id_vars=["Mitochondria"], var_name="Measurement", value_name=measurement_name)
            file_df.set_index(["Mitochondria", "Measurement"], inplace = True)
            file_df.sort_index(inplace=True)

            # Drop empty rows?
            file_df.dropna(inplace=True)

            # Add summary columns
            file_df.reset_index(level='Measurement', inplace=True)
            for col in summary_df.columns:
                file_df[col] = summary_df[col]
            file_df.reset_index(inplace=True)
            file_df.set_index(["Mitochondria", "Measurement"], inplace = True)

            if run_df is None:
                run_df = file_df
            else:
                for col in file_df:
                    run_df[col] = file_df[col]

            Joana.loop_tick()

        run_df["Run ID"] = Joana.run_name
        run_df["Run Type"] = Joana.run_name.split("_")[0]
        run_df["Run Number"] = Joana.run_name.split("_")[1]

        if not Joana.data_directory.exists():
            Joana.data_directory.mkdir()
        run_df.to_csv(Joana.data_directory/"all_data.csv")

        with open(Joana.data_directory/"all_measurements.pkl", 'wb') as pickle_file:
            pickle.dump(all_measurements, pickle_file)

def script_main(
                mitometer_path: Path,
                run_name: str,
                output_path: Path,
                marginal_type: str = "rug",
                disable_plots: bool = False,
                ):
    logger = logging.getLogger('read_mitometer_files')

    with RM.RunManager(output_path / run_name) as Tiago:
        Tiago.create_run(raise_error=False)

        # Backup files for later reference
        for file in mitometer_path.iterdir():
            if not file.is_file():
                continue
            if file.suffix != ".txt":
                continue
            Tiago.backup_file(file)

        read_mitometer_task(Tiago, mitometer_path, logger)
        if not disable_plots:
            plot_summary_task(Tiago, logger, marginal_type)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
                    prog='read_mitometer_file.py',
                    description='This script reads the translated mitometer files in txt format',
                    #epilog='Text at the bottom of help'
                    )

    parser.add_argument(
        '-m',
        '--mitometer',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the directory containing the translated mitometer files in txt format',
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