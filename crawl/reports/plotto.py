import os
import re
import json
import pprint
import datetime
import plotly.offline as py
import plotly.graph_objs as go
from jinja2 import Environment, FileSystemLoader

ALL = 'All'
S200 = '200 Found'
S403 = 'Access denied'
SM9998 = '-9998 Blocked by robots.txt'
CAPPED = 'Hit data cap'
DEDUP = 'De-duplicated responses'
BYTES = 'Bytes downloaded'


def load_timeline(summary_file):
    hours = []
    statistics = []
    with open(summary_file) as f:
        for line in f:
            key, json_str = re.split('\t', line, maxsplit=1)
            if key.startswith("BY-HOUR"):
                tag, time = re.split(' ', key, maxsplit=1)
                stats = json.loads(json_str)
                # Store
                hours.append(time)
                statistics.append(stats)

    # And return the two lists:
    return hours, statistics


def load_targets(summary_file):
    """

    :param summary_file:
    :return:
    """
    targets = {}
    with open(summary_file) as f:
        for line in f:
            key, json_str = re.split('\t', line, maxsplit=1)
            if key.startswith("BY-TARGET"):
                tag, tid = re.split(' ', key, maxsplit=1)
                stats = json.loads(json_str)
                targets[tid] = stats

    return targets


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def plot_status_codes_timeline(hours, statistics):
    # Scan for all status codes:
    status_codes = set()
    for stats in statistics:
        for key in stats:
            if key.startswith('status_code:'):
                status_codes.add(key[12:])
    # For now, limit this to codes of particular interest:
    status_codes = ['200', '301', '302', '401', '403', '404', '-1', '-9998']
    # Now build up timeline:
    status_over_time = {}
    for code in status_codes:
        status_over_time[code] = []
    for stats in statistics:
        for code in status_codes:
            count = stats.get('status_code:%s' % code, 0)
            status_over_time[code].append(count)
    # And plot it:
    traces = []
    for code in status_codes:
        traces.append(go.Scatter(x=hours, y=status_over_time[code], name=code))

    layout = go.Layout(
        title='Major status codes over time')

    figure = go.Figure(data=traces, layout=layout)
    return figure


def plot_dedup_timeline(hours, statistics):
    lines = {}
    title = {}
    lines[ALL] = []
    title[ALL] = 'Total events over time'
    lines[DEDUP] = []
    title[DEDUP] = 'URLs deduplicated'
    for stats in statistics:
        lines[ALL].append(stats['lines'])
        lines[DEDUP].append(stats.get('duplicate:digest', None))

    # And plot:
    data = []
    for key in lines:
        data.append(go.Scatter(x=hours, y=lines[key], name=title[key]))

    return {'data': data,
                'layout': {'title': 'Total of all crawl events and deduplication', 'height': 300}
              }


def plot_single_timelines(hours, statistics):
    lines = {}
    title = {}
    lines[CAPPED] = []
    title[CAPPED] = 'Sites hitting their data cap'
    lines[BYTES] = []
    title[BYTES] = 'Bytes downloaded'
    for stats in statistics:
        lines[CAPPED].append(stats.get('Q:serverMaxSuccessKb', None))
        lines[BYTES].append(stats.get('sum:content_length', 0))

    # And plot:
    plots = []
    for key in lines:
        plots.append({'data': [go.Scatter(x=hours, y=lines[key])],
                  'layout': {'title': title[key], 'height': 300}
                  })
    return plots


def pie_for(stats, prefix, title, label_max_length=40, max_slices=20):
    labels = []
    values = []
    total = 0
    for key in stats:
        if key.startswith(prefix):
            label = key[len(prefix):]
            if len(label) > label_max_length:
                label = "...%s" % label[-label_max_length:]
            if total < max_slices:
                labels.append(label)
                values.append(stats[key])
            total += 1

    layout = go.Layout(
        legend=dict(
            bgcolor='rgba(1.0,1.0,1.0,0.25)',
            borderwidth=0
        ),
        title="%s (%i in total)" % (title, total),
        height=400
    )

    trace = go.Pie(labels=labels, values=values, textinfo='none')
    fig = go.Figure(data=[trace], layout=layout)
    return fig

# Capture our current directory
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),'templates')


def get_plot_div(figure, include_plotlyjs=True):
    return py.plot(figure, output_type='div', include_plotlyjs=include_plotlyjs, auto_open=False, show_link=False)


def generate_crawl_summary(job, launch, summary_file, reports_folder):
    # Basic properties:
    job_name = job.capitalize()
    launch_date = datetime.datetime.strptime( launch, "%Y%m%d%H%M%S" )
    # Load in data:
    hours, stats = load_timeline(summary_file)
    target_stats = load_targets(summary_file)
    # Build up mapping to seeds:
    target_seeds = {}
    for target in target_stats:
        target_seeds[target] = []
        for stat in target_stats[target]:
            if stat.startswith('source:'):
                target_seeds[target].append(stat[7:])
        # Reformat some properties:
        total_bytes = int(target_stats[target].get('sum:content_length',0))
        target_stats[target]['sum:content_length'] = "{:,.0f}".format(total_bytes)

    # Make some graphs
    figure = plot_status_codes_timeline(hours,stats)
    timeline = get_plot_div(figure)

    # Create the jinja2 environment.
    j2_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR),
                         trim_blocks=True)
    out = j2_env.get_template('crawl_summary.html').render(
        title='Crawl Summary For %s Crawl Launched %s' % (job_name, launch_date),
        target_stats=target_stats,
        target_seeds=target_seeds,
        timelines=timeline
    )

    #pprint.pprint(target_stats['21959'])

    report_folder = os.path.join(reports_folder, job, launch)
    if not os.path.isdir(report_folder):
        os.makedirs(report_folder)
    output_file = os.path.join(report_folder, 'index.html')

    with open(output_file, 'w') as f:
        f.write(out)

    target_template = j2_env.get_template('target_summary.html')

    target_report_folder = os.path.join(report_folder, 'targets')
    if not os.path.isdir(target_report_folder):
        os.makedirs(target_report_folder)
    for tid in target_stats:
        output_file = os.path.join(target_report_folder, '%s.html' % tid)
        print(output_file)
        status_codes_pie = get_plot_div(pie_for(target_stats[tid], 'status_code:', 'Status Codes'))
        tries_pie = get_plot_div(pie_for(target_stats[tid], 'tries:', 'Retries'), include_plotlyjs=False)
        hops_pie = get_plot_div(pie_for(target_stats[tid], 'hop:', 'Crawl Hops'), include_plotlyjs=False)
        hosts_pie =get_plot_div(pie_for(target_stats[tid], 'host:', 'Hosts'), include_plotlyjs=False)
        ip_pie = get_plot_div(pie_for(target_stats[tid], 'ip:', 'IP Addresses'), include_plotlyjs=False)
        mime_pie = get_plot_div(pie_for(target_stats[tid], 'content_type:', 'Content Types'), include_plotlyjs=False)

        out = target_template.render(
            title='Target Summary For %s from %s Crawl Launched %s' % (tid, job_name, launch_date),
            tid=tid,
            seeds=target_seeds[tid],
            stats=target_stats[tid],
            status_codes=status_codes_pie,
            hops=hops_pie,
            tries=tries_pie,
            hosts=hosts_pie,
            ips=ip_pie,
            types=mime_pie
        )
        with open(output_file, 'w') as f:
            f.write(out)





if __name__ == '__main__':
    a_summary_file = '../../tasks/process/test-data/weekly-20170220090024-crawl-logs-14.analysis.tsjson.sorted'
    generate_crawl_summary('weekly', '20170220090024', a_summary_file, '/Users/andy/Documents/workspace/pulse/testing/reports')



