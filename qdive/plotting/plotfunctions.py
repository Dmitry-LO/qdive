import numpy as np
import matplotlib.pyplot as plt


def plot_data(data, scatter, **kwargs):

    nsig = kwargs.get("nsig")
    x = kwargs.get("x", "")
    y = kwargs.get("y","") 

    PlXax = scatter[x+"_mean"]
    PlYax = scatter[y+"_mean"]
    PlXax2 = data[x+"_mean"]
    PlYax2= data[y+"_mean"]
    PlXer2 = data[x+"_std"]
    PlYer2 = data[y+"_std"]


    # Define colors and palettes
    Pal1 = np.array([
        [89, 188, 206],
        [0, 115, 189],
        [217, 83, 25],
        [225, 122, 0],
        [255, 215, 0]
    ]) / 255

    Pal3 = np.array([
        [254, 239, 229],
        [0, 145, 110],
        [0, 115, 189],
        [217, 83, 25],
        [255, 207, 0]
    ]) / 255

    # Choose palette
    Pal = Pal3

    # Plot settings
    sc1 = 1  # Scaling
    xlabelN = x #'Sample temperature T (K)'
    ylabelN = y #'Surface resistance Rs (nOhm)'
    freq = 416.0*10**6 #str(round(PlotModeData[0]["dataline"][0][1] / 1e6, 0))
    plot1name = f"{freq} MHz, B test mT Run 0"

    MarkSize = 7 * sc1
    LineW = kwargs.get("LineW", 1.5) * sc1
    FontS = 18 * sc1
    MarkShape = 'o'
    MarkColor = 'none'
    Lcol = Pal[3]

    # Create figure
    plt.figure(figsize=(9 * sc1, 6 * sc1))

    # Plot data

    plt.errorbar(PlXax2, PlYax2, xerr = nsig * PlXer2, yerr = nsig * PlYer2,
                    fmt='s',
                    label=plot1name,
                    linewidth=LineW,
                    color=Lcol,
                    markeredgecolor='black',
                    markerfacecolor=Pal[2],
                    capsize=3,
                    ecolor='black',
                    markersize=6,
                    markeredgewidth=1.0)

    plt.plot(PlXax, PlYax,
                MarkShape,
                label=plot1name,
                linewidth=LineW,
                color=Lcol,
                markeredgecolor=Lcol,
                markerfacecolor=MarkColor,
                markersize=MarkSize,
                markeredgewidth=1.0)

    # Finalize plot
    plt.xlabel(xlabelN, fontsize=FontS, fontname='serif')
    plt.ylabel(ylabelN, fontsize=FontS, fontname='serif')
    plt.grid(False)
    plt.box(True)
    # Adjust tick parameters
    ax = plt.gca()
    ax.tick_params(width=1, labelsize=FontS)
    for label in ax.get_xticklabels():
        label.set_fontsize(FontS)
        label.set_fontname('serif')
    for label in ax.get_yticklabels():
        label.set_fontsize(FontS)
        label.set_fontname('serif')

    plt.gca().spines['top'].set_linewidth(1)
    plt.gca().spines['right'].set_linewidth(1)
    plt.gca().spines['left'].set_linewidth(1)
    plt.gca().spines['bottom'].set_linewidth(1)


    # ax.set_xlim([
    #     np.min(PlXax) - np.ceil(np.max(PlXax) * 0.05),
    #     np.max(PlXax) + np.ceil(np.max(PlXax) * 0.05)
    # ])
    plt.tight_layout()
    plt.show()