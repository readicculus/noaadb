import os

import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image, ImageDraw

# Settings
matplotlib.rc('font', **{'size': 11})
matplotlib.use('Agg')  # for writing to files only

# A lot of this code taken/adapted from https://github.com/ultralytics/yolov5/blob/master/utils/plots.py

def color_list():
    # Return first 10 plt colors as (r,g,b) https://stackoverflow.com/questions/51350872/python-from-color-name-to-rgb
    def hex2rgb(h):
        return tuple(int(h[1 + i:1 + i + 2], 16) for i in (0, 2, 4))

    return [hex2rgb(h) for h in plt.rcParams['axes.prop_cycle'].by_key()['color']]
def show_values_on_bars(axs):
    def _show_on_single_plot(ax):
        for p in ax.patches:
            _x = p.get_x() + p.get_width() / 2
            _y = p.get_y() + p.get_height()
            value = p.get_height()
            if p.get_height().is_integer():
                value = '%d' % p.get_height()
            else:
                value = '{:.2f}'.format(p.get_height())
            ax.text(_x, _y, value, ha="center")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _show_on_single_plot(ax)
    else:
        _show_on_single_plot(axs)

def draw_label_plots(x, out_dir, names, fn_prefix=''):
    if fn_prefix != '': fn_prefix += '_'
    sns.pairplot(x[['x', 'y', 'w', 'h']], corner=True, diag_kind='auto', kind='hist', diag_kws=dict(bins=50),
                 plot_kws=dict(pmax=0.9))
    plt.savefig(os.path.join(out_dir, '%slabels_correlogram.jpg'%fn_prefix), dpi=200)
    plt.close()
    # matplotlib labels
    nc = len(names)
    ax = plt.subplots(2, 2, figsize=(8, 8), tight_layout=True)[1].ravel()
    ax[0].hist(x['class_id'].astype(np.int).to_list(), bins=np.linspace(0, nc, nc + 1) - 0.5, rwidth=0.8)
    ax[0].set_xlabel('classes')
    ticks = []
    for t in ax[0].get_xticks():
        if t.is_integer():
            ticks.append(names[int(t)])
        else:
            ticks.append('')
    ax[0].set_xticklabels(ticks)
    show_values_on_bars(ax[0])
    sns.histplot(x, x='x', y='y', ax=ax[2], bins=50, pmax=0.9)
    sns.histplot(x, x='w', y='h', ax=ax[3], bins=50, pmax=0.9)

    # rectangles
    colors = color_list()
    x['x'] = 0.5  # center
    x['y'] = 0.5  # center
    x1 = x['x'] - (x['w'] / 2.)
    x2 = x['x'] + (x['w'] / 2.)
    y1 = x['y'] - (x['h'] / 2.)
    y2 = x['y'] + (x['h'] / 2.)
    labels = np.array([x['class_id'], x1, y1, x2, y2]).T
    labels[:, 1:] *= 2000

    img = Image.fromarray(np.ones((2000, 2000, 3), dtype=np.uint8) * 255)
    for row in labels[:1000]:
        ImageDraw.Draw(img).rectangle(row[1:], width=1, outline=colors[int(row[0]) % 10])  # plot
    ax[1].imshow(img)
    ax[1].axis('off')

    for a in [0, 1, 2, 3]:
        for s in ['top', 'right', 'left', 'bottom']:
            ax[a].spines[s].set_visible(False)

    plt.savefig(os.path.join(out_dir, '%slabels.jpg'%fn_prefix), dpi=200)
    matplotlib.use('Agg')
    plt.close()