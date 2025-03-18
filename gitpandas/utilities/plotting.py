"""
.. module:: plotting
   :platform: Unix, Windows
   :synopsis: helper functions for plotting tables from gitpandas

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

try:
    import matplotlib.pyplot as plt
    import matplotlib.style
    matplotlib.style.use('ggplot')
    HAS_MPL = True
except ImportError as e:
    HAS_MPL = False

__author__ = 'willmcginnis'


def plot_punchcard(df, metric='lines', title='punchcard', by=None):
    """
    Uses modified plotting code from https://bitbucket.org/birkenfeld/hgpunchcard

    :param df:
    :param metric:
    :param title:
    :return:
    """

    if not HAS_MPL:
        raise ImportError('Must have matplotlib installed to use the plotting functions')

    # find how many plots we are making
    if by is not None:
        unique_vals = set(df[by].values.tolist())
    else:
        unique_vals = ['foo']
    for idx, val in enumerate(unique_vals):
        if by is not None:
            sub_df = df[df[by] == val]
        else:
            sub_df = df
        fig = plt.figure(figsize=(8, title and 3 or 2.5), facecolor='#ffffff')
        ax = fig.add_subplot(111, facecolor='#ffffff')
        fig.subplots_adjust(left=0.06, bottom=0.04, right=0.98, top=0.95)
        if by is not None:
            ax.set_title(title + ' (%s)' % (str(val), ), y=0.96).set_color('#333333')
        else:
            ax.set_title(title, y=0.96).set_color('#333333')
        ax.set_frame_on(False)
        ax.scatter(sub_df['hour_of_day'], sub_df['day_of_week'], s=sub_df[metric], c='#333333', edgecolor='#333333')
        for line in ax.get_xticklines() + ax.get_yticklines():
            line.set_alpha(0.0)
        dist = -0.8
        ax.plot([dist, 23.5], [dist, dist], c='#555555')
        ax.plot([dist, dist], [dist, 6.4], c='#555555')
        ax.set_xlim(-1, 24)
        ax.set_ylim(-0.9, 6.9)
        ax.set_yticks(range(7))
        for tx in ax.set_yticklabels(['Mon', 'Tues', 'Wed', 'Thurs', 'Fri', 'Sat', 'Sun']):
            tx.set_color('#555555')
            tx.set_size('x-small')
        ax.set_xticks(range(24))
        for tx in ax.set_xticklabels(['%02d' % x for x in range(24)]):
            tx.set_color('#555555')
            tx.set_size('x-small')
        ax.set_aspect('equal')
        if idx + 1 == len(unique_vals):
            plt.show(block=True)
        else:
            plt.show(block=False)


def plot_cumulative_blame(df):
    """
    Plot cumulative blame information as a stacked area chart.

    Args:
        df (pandas.DataFrame): DataFrame with dates as index and committers as columns

    Returns:
        matplotlib.figure.Figure: The generated figure
    """

    if not HAS_MPL:
        raise ImportError('Must have matplotlib installed to use the plotting functions')

    ax = df.plot(kind='area', stacked=True)
    plt.title('Cumulative Blame')
    plt.xlabel('date')
    plt.ylabel('LOC')
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    
    # Only try to show if not using Agg backend
    if plt.get_backend() != 'Agg':
        plt.show()
    
    return plt.gcf()


def plot_lifeline(changes, ownership_changes, refactoring):
    """
    Plot file lifelines with ownership changes and refactoring events.

    Args:
        changes (pd.DataFrame): DataFrame containing file change history
        ownership_changes (pd.DataFrame): DataFrame containing ownership change events
        refactoring (pd.DataFrame): DataFrame containing refactoring events

    Returns:
        matplotlib.figure.Figure: The generated plot figure
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        raise ImportError("matplotlib is required for plotting. Please install it first.")

    # Create a new figure
    fig, ax = plt.subplots(figsize=(12, 6))

    # Get unique files and sort them
    files = changes.filename.unique()
    files.sort()

    # Create y-axis positions for each file
    file_positions = {file: i for i, file in enumerate(files)}

    # Plot lifelines
    for file in files:
        file_changes = changes[changes.filename == file]
        ax.plot(file_changes.index, [file_positions[file]] * len(file_changes),
                '-', label='_nolegend_', alpha=0.5)

    # Plot ownership changes
    if len(ownership_changes) > 0:
        ax.scatter(ownership_changes.index,
                  [file_positions[f] for f in ownership_changes.filename],
                  marker='o', c='red', label='Ownership Change', alpha=0.7)

    # Plot refactoring events
    if len(refactoring) > 0:
        ax.scatter(refactoring.index,
                  [file_positions[f] for f in refactoring.filename],
                  marker='s', c='blue', label='Refactoring', alpha=0.7)

    # Customize the plot
    ax.set_yticks(range(len(files)))
    ax.set_yticklabels(files)
    ax.set_xlabel('Time')
    ax.set_ylabel('Files')
    ax.set_title('File Lifelines with Ownership Changes and Refactoring Events')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Rotate dates for better readability
    plt.xticks(rotation=45)

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    return fig