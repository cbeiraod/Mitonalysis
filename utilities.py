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
import enum
import datetime
import sqlite3
import hashlib

import plotly.express as px
import plotly.graph_objects as go

import pandas

myMeasurementDict = {
    "Volume": {
        "label": r"$\text{Volume }[\mu m^3]$",
    },
    "MajorAxisLength": {
        "label": r"$\text{Major Axis Length }[\mu m]$",
    },
    "MinorAxisLength": {
        "label": r"$\text{Minor Axis Length }[\mu m]$",
    },
    "ZAxisLength": {
        "label": r"$\text{Z Axis Length }[\mu m]$",
    },
    "SurfaceArea": {
        "label": r"$\text{Surface Area }[\mu m^2]$",
    },
    "Solidity": {
        "label": r"$\text{Solidity }[\mu m]$",
    },
    "velocity": {
        "label": r"$\text{Velocity }[\mu m/s]$",
    },
    "speed": {
        "label": r"$\text{Speed }[\mu m/s]$",
    },
    "MeanIntensity": {
        "label": r"$\text{Mean Intensity}$",
    },
    "distance": {
        "label": r"$\text{Distance }[\mu m]$",
    },
    "displacement": {
        "label": r"$\text{Displacement }[\mu m]$",
    },
}

def measurement_to_label(measurement: str):
    if measurement not in myMeasurementDict:
        raise RuntimeError(f"Unknown measurement: {measurement}")
    return myMeasurementDict[measurement]["label"]

def get_sorted_measurements_from_path(mitometer_path: Path, logger: logging.Logger, first_measurements: list[str] = [], raise_exception: bool = False):
    file_list = []
    first_measurement_file = {}
    for measurement in first_measurements:
        first_measurement_file[measurement] = None

    for file in mitometer_path.iterdir():
        if not file.is_file():
            continue
        if file.suffix != ".txt":
            continue
        file_list += [file]

        if len(first_measurements) > 0:
            file_name: str = file.name
            measurement_index = file_name.find(".tif_")
            if measurement_index >= 0:
                measurement_name = file_name[measurement_index+5:-4]
            else:
                measurement_name = file_name
            if measurement_name in first_measurements:
                first_measurement_file[measurement_name] = file

    file_list.sort()

    if len(first_measurements) > 0:
        # Bring forth the first first measurement found, just to guarantee that specific files are processed first
        bring_forth = None
        for measurement in first_measurements:
            bring_forth = first_measurement_file[measurement]
            if bring_forth is not None:
                break

        if bring_forth is None:
            error_msg = "Could not find one of the first measurements to bring to the front of the list"
            if not raise_exception:
                logger.warn(error_msg)
            else:
                raise RuntimeError(error_msg)
        else:
            old_list = file_list
            file_list = [bring_forth]
            for file in old_list:
                if file not in file_list:
                    file_list += [file]

    return file_list

def make_multiscatter_plot(
    data_df:pandas.DataFrame,
    run_name: str,
    base_path: Path,
    dimensions: list[str],
    title: str = "Scatter plot comparing variables",
    labels: dict[str, str] = {},
    color_var: str = None,
    symbol_var: str = None,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    extra_title: str = "",
    file_name: str = "multi_scatter",
    opacity: float = 0.7,
    marker_size: float = 2,
    ):

    fig = px.scatter_matrix(
        data_df,
        dimensions = sorted(dimensions),
        labels = labels,
        color = color_var,
        symbol = symbol_var,
        title = "{}<br><sup>Run: {}{}</sup>".format(title, run_name, extra_title),
        opacity = opacity,
    )

    fig.update_traces(
        diagonal_visible=False,
        showupperhalf=False,
        marker = {'size': marker_size},
    )
    for k in range(len(fig.data)):
        fig.data[k].update(
            selected = dict(
                marker = dict(
                    #opacity = 1,
                    #color = 'blue',
                )
            ),
            unselected = dict(
                marker = dict(
                    #opacity = 0.1,
                    color="grey"
                )
            ),
        )

    # This is the workaround of plotly not supporting latex in hover labels, by changing only the title at the end
    # this workaround worked for histograms but not for scatters... :(
    #fig.update_layout({"xaxis{}".format(i+1): dict(title = labels[dimensions[i]]) for i in range(len(labels))})
    #fig.update_layout({"yaxis{}".format(i+1): dict(title = labels[dimensions[i]], tickangle = -45) for i in range(len(labels))})

    fig.write_html(
        base_path/f'{file_name}.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
        include_mathjax = 'cdn',
    )

def make_histogram_plot(
    data_df: pandas.DataFrame,
    x_var: str,
    base_path: Path,
    file_name: str,
    run_name: str,
    group_var: str = None,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    nbins: int = None,
    logy: bool = False,
    x_label: str = None,
    marginal_type: str = None,
    facet_col_var: str = None,
    facet_col_wrap: int = None,
    facet_row_var: str = None,
    #facet_row_wrap: int = None,
    pattern_shape_var: str = None,
    min_x: float = None,
    max_x: float = None,
    extra_title: str = "",
    ):
    make_histogram_plot_type_choice(
        hist_type = "count",
        data_df = data_df,
        x_var = x_var,
        base_path = base_path,
        file_name = file_name + "_histogram",
        run_name = run_name,
        group_var = group_var,
        full_html = full_html,
        nbins = nbins,
        logy = logy,
        x_label = x_label,
        marginal_type = marginal_type,
        facet_col_var = facet_col_var,
        facet_col_wrap = facet_col_wrap,
        facet_row_var = facet_row_var,
        #facet_row_wrap = facet_row_wrap,
        pattern_shape_var = pattern_shape_var,
        min_x = min_x,
        max_x = max_x,
        extra_title = extra_title,
    )

    if group_var is not None or pattern_shape_var is not None:
        make_histogram_plot_type_choice(
            hist_type = "pdf",
            data_df = data_df,
            x_var = x_var,
            base_path = base_path,
            file_name = file_name + "_pdf",
            run_name = run_name,
            group_var = group_var,
            full_html = full_html,
            nbins = nbins,
            logy = logy,
            x_label = x_label,
            marginal_type = marginal_type,
            facet_col_var = facet_col_var,
            facet_col_wrap = facet_col_wrap,
            facet_row_var = facet_row_var,
            #facet_row_wrap = facet_row_wrap,
            pattern_shape_var = pattern_shape_var,
            min_x = min_x,
            max_x = max_x,
            extra_title = extra_title,
        )

def make_histogram_plot_type_choice(
    hist_type: str,
    data_df: pandas.DataFrame,
    x_var: str,
    base_path: Path,
    file_name: str,
    run_name: str,
    group_var: str = None,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    nbins: int = None,
    logy: bool = False,
    x_label: str = None,
    marginal_type: str = None,
    facet_col_var: str = None,
    facet_col_wrap: int = None,
    facet_row_var: str = None,
    #facet_row_wrap: int = None,
    pattern_shape_var: str = None,
    min_x: float = None,
    max_x: float = None,
    extra_title: str = "",
    ):
    if hist_type not in ["count", "pdf"]:
        raise RuntimeError("Unknown histogram type")

    if extra_title != "":
        extra_title = "<br>" + extra_title

    opacity = 1
    if group_var is not None:
        opacity = 0.7

    y_label = "Count"
    histnorm = None
    if hist_type == "pdf":
        y_label = "Probability"
        histnorm = "probability"
    labels = {
        "count": y_label,
    }
    #if x_label is not None:  # This is the correct way, but plotly does not yet support latex in hover labels
    #    labels[x_var] = x_label

    range_x = None
    if min_x is not None and max_x is not None:
        range_x = [min_x, max_x]

    fig = px.histogram(
        data_frame = data_df,
        x = x_var,
        nbins = nbins,
        opacity = opacity,
        log_y = logy,
        labels = labels,
        range_x = range_x,
        color = group_var,
        barmode = "overlay",
        marginal = marginal_type,
        facet_col = facet_col_var,
        facet_col_wrap = facet_col_wrap,
        facet_row = facet_row_var,
        #facet_row_wrap = facet_row_wrap,
        pattern_shape = pattern_shape_var,
        histnorm = histnorm,
    )

    fig.update_layout(
        title_text="Histogram of {}<br><sup>Run: {}{}</sup>".format(x_var, run_name, extra_title),
        yaxis_title=y_label
    )

    # This is the workaround of plotly not supporting latex in hover labels, by changing only the title at the end
    if x_label is not None:
        fig.update_layout(
            xaxis_title=x_label,
        )

    fig.write_html(
        base_path/'{}.html'.format(file_name),
        full_html = full_html,
        include_plotlyjs = 'cdn',
        include_mathjax = 'cdn',
    )

def make_box_plot(
    data_df: pandas.DataFrame,
    x_var: str,
    base_path: Path,
    file_name: str,
    run_name: str,
    group_var: str = None,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    x_label: str = None,
    facet_col_var: str = None,
    facet_col_wrap: int = None,
    facet_row_var: str = None,
    #facet_row_wrap: int = None,
    pattern_shape_var: str = None,
    min_x: float = None,
    max_x: float = None,
    extra_title: str = "",
    ):
    if extra_title != "":
        extra_title = "<br>" + extra_title

    range_x = None
    if min_x is not None and max_x is not None:
        range_x = [min_x, max_x]

    fig = px.box(
        data_frame = data_df,
        x = x_var,
        y = pattern_shape_var,
        color = group_var,
        notched = True,
        facet_col = facet_col_var,
        facet_col_wrap = facet_col_wrap,
        facet_row = facet_row_var,
        range_x = range_x,
    )

    fig.update_layout(
        title_text="Box plot of {}<br><sup>Run: {}{}</sup>".format(x_var, run_name, extra_title),
    )

    # This is the workaround of plotly not supporting latex in hover labels, by changing only the title at the end
    if x_label is not None:
        fig.update_layout(
            xaxis_title=x_label,
        )

    fig.write_html(
        base_path/'{}_box.html'.format(file_name),
        full_html = full_html,
        include_plotlyjs = 'cdn',
        include_mathjax = 'cdn',
    )

def make_violin_plot(
    data_df: pandas.DataFrame,
    x_var: str,
    base_path: Path,
    file_name: str,
    run_name: str,
    group_var: str = None,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    x_label: str = None,
    facet_col_var: str = None,
    facet_col_wrap: int = None,
    facet_row_var: str = None,
    #facet_row_wrap: int = None,
    pattern_shape_var: str = None,
    min_x: float = None,
    max_x: float = None,
    extra_title: str = "",
    ):
    if extra_title != "":
        extra_title = "<br>" + extra_title

    range_x = None
    if min_x is not None and max_x is not None:
        range_x = [min_x, max_x]

    fig = px.violin(
        data_frame = data_df,
        x = x_var,
        y = pattern_shape_var,
        color = group_var,
        facet_col = facet_col_var,
        facet_col_wrap = facet_col_wrap,
        facet_row = facet_row_var,
        range_x = range_x,
    )

    fig.update_layout(
        title_text="Violin plot of {}<br><sup>Run: {}{}</sup>".format(x_var, run_name, extra_title),
    )

    # This is the workaround of plotly not supporting latex in hover labels, by changing only the title at the end
    if x_label is not None:
        fig.update_layout(
            xaxis_title=x_label,
        )

    fig.write_html(
        base_path/'{}_violin.html'.format(file_name),
        full_html = full_html,
        include_plotlyjs = 'cdn',
        include_mathjax = 'cdn',
    )

if __name__ == "__main__":
    raise RuntimeError("Do not try to run this file, it is not a standalone script. It contains several common utilities used by the other scripts")
