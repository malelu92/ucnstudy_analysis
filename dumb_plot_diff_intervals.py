import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict

window_error = ['0s', '15s', '30', '45', '1m', '2', '3', '4', '5m' ]
trace_intervals = ['1', '3', '5', '7', '10']

def plot_sliding_window():

    x_0 = defaultdict(list)
    y_0 = defaultdict(list)

    x_15 = defaultdict(list)
    y_15 = defaultdict(list)

    x_30 = defaultdict(list)
    y_30 = defaultdict(list)
    
    x_45 = defaultdict(list)
    y_45 = defaultdict(list)

    x_60 = defaultdict(list)
    y_60 = defaultdict(list)

    x_error_120 = defaultdict(list)
    y_error_120 = defaultdict(list)

    x_error_180 = defaultdict(list)
    y_error_180 = defaultdict(list)

    x_error_240 = defaultdict(list)
    y_error_240 = defaultdict(list)

    x_error_300 = defaultdict(list)
    y_error_300 = defaultdict(list)

    #x_0 and y_0 ==================

    x_0_not_filtered = [56.6178338555, 54.6791830349, 55.1466793696, 55.9359825056, 55.781154069]
    y_0_not_filtered = [3.1646708004, 5.07297139521, 6.57547214465, 8.23290891592, 10.1776557643]

    #x_0_interval_filtered = [68.0074836296, 64.1031315702, 62.7486724261, 61.8052030457, 61.0512616586]
    #y_0_interval_filtered = [0.917625155804, 1.82799261608, 2.49822502012, 3.07363405437, 3.99678136981]

    #x_0_blist_filtered = [56.6634269053, 54.7677725118, 55.2277238346, 56.0311199487, 55.8027739834]
    #y_0_blist_filtered = [3.12743566684, 5.01396317508, 6.49721525378, 8.13587668229, 10.0549060444]

    x_0_filtered = [68.064936603, 64.2893131053, 62.8856478885, 62.0054548372, 61.1828638269]
    y_0_filtered = [0.906265284549, 1.81189946514, 2.47629415756, 3.04886322399, 3.96128177214]

    #x_15 and y_15 ==================

    x_15_filtered = [87.7236639412, 82.7968426356, 81.2404840131, 79.618161399, 78.1996295935]
    y_15_filtered = [1.16801565138, 2.33350688692, 3.19906596614, 3.91489563118, 5.06303150787]

    x_15_not_filtered = [73.5563082134, 70.8022990851, 71.6814861861, 71.8415693, 71.6047247635]
    y_15_not_filtered = [4.11180006627, 6.56916110506, 8.54735646329, 10.5743046023, 13.0651141509]

    x_15_blist_filtered = []
    y_15_blist_filtered = []

    x_15_interval_filtered = []
    y_15_interval_filtered = []

    #x_30 and y_30 ==================

    x_30_not_filtered = [76.0880609653, 73.2935414754, 74.1531703186, 74.2180297995, 73.9800771319]
    y_30_not_filtered = [4.25332512898, 6.8003029299, 8.84208201196, 10.9240939714, 13.4985247945]

    x_30_filtered = [89.0863846427, 84.7282091474, 83.211795817, 81.6268249639, 80.0565357247]
    y_30_filtered = [1.18615989019, 2.38793960335, 3.27669175305, 4.0136634007, 5.18325681198]

    x_30_blist_filtered = []
    y_30_blist_filtered = []

    x_30_interval_filtered = []
    y_30_interval_filtered = []

    #x_45 and y_45 ==================

    x_45_not_filtered = [77.8859723398, 74.8716117403, 75.6999576585, 75.6779933541, 75.393009702]
    y_45_not_filtered = [4.35382843439, 6.9467190483, 9.02652214386, 11.1389848693, 13.7563307616]

    x_45_filtered = [90.1647114587, 85.7694676146, 84.4659027166, 82.8365153217, 81.1775026806]
    y_45_filtered = [1.20051750525, 2.41728593743, 3.32607563781, 4.0731449488, 5.25583376722]

    x_45_blist_filtered = []
    y_45_blist_filtered = []

    x_45_interval_filtered = []
    y_45_interval_filtered = []

    #x_60 and y_60 ==================

    x_60_not_filtered = [78.7750493932, 75.9293269394, 76.8511167566, 76.913924322, 76.4738944711]
    y_60_not_filtered = [4.40352787113, 7.04485571386, 9.16378725486, 11.3209005853, 13.9535507486]

    x_60_filtered = [90.6861002488, 86.4916307451, 85.4515586185, 83.8600994706, 82.1108295155]
    y_60_filtered = [1.20745964879, 2.43763904009, 3.36488853126, 4.1234754895, 5.31626197125]

    x_60_blist_filtered = []
    y_60_blist_filtered = []

    x_60_interval_filtered = []
    y_60_interval_filtered = []


    #x_0['blist'] = divide_value_per_100(x_0_blist_filtered)
    #y_0['blist'] = divide_value_per_100(y_0_blist_filtered)

    #x_0['interval'] = divide_value_per_100(x_0_interval_filtered)
    #y_0['interval'] = divide_value_per_100(y_0_interval_filtered)

    x_0['not_filtered'] = divide_value_per_100(x_0_not_filtered)
    y_0['not_filtered'] = divide_value_per_100(y_0_not_filtered)

    x_0['filtered'] = divide_value_per_100(x_0_filtered)
    y_0['filtered'] = divide_value_per_100(y_0_filtered)


    #x_15['blist'] = divide_value_per_100(x_15_blist_filtered)
    #y_15['blist'] = divide_value_per_100(y_15_blist_filtered)

    #x_15['interval'] = divide_value_per_100(x_15_interval_filtered)
    #y_15['interval'] = divide_value_per_100(y_15_interval_filtered)

    x_15['not_filtered'] = divide_value_per_100(x_15_not_filtered)
    y_15['not_filtered'] = divide_value_per_100(y_15_not_filtered)

    x_15['filtered'] = divide_value_per_100(x_15_filtered)
    y_15['filtered'] = divide_value_per_100(y_15_filtered)


    #x_30['blist'] = divide_value_per_100(x_10_blist_filtered)
    #y_30['blist'] = divide_value_per_100(y_10_blist_filtered)

    #x_30['interval'] = divide_value_per_100(x_10_interval_filtered)
    #y_30['interval'] = divide_value_per_100(y_10_interval_filtered)

    x_30['not_filtered'] = divide_value_per_100(x_30_not_filtered)
    y_30['not_filtered'] = divide_value_per_100(y_30_not_filtered)

    x_30['filtered'] = divide_value_per_100(x_30_filtered)
    y_30['filtered'] = divide_value_per_100(y_30_filtered)


    x_45['not_filtered'] = divide_value_per_100(x_45_not_filtered)
    y_45['not_filtered'] = divide_value_per_100(y_45_not_filtered)

    x_45['filtered'] = divide_value_per_100(x_45_filtered)
    y_45['filtered'] = divide_value_per_100(y_45_filtered)


    x_60['not_filtered'] = divide_value_per_100(x_60_not_filtered)
    y_60['not_filtered'] = divide_value_per_100(y_60_not_filtered)

    x_60['filtered'] = divide_value_per_100(x_60_filtered)
    y_60['filtered'] = divide_value_per_100(y_60_filtered)

    plot_roc_curve (x_0, y_0, x_15, y_15, x_30, y_30, x_45, y_45, x_60, y_60)

def plot_roc_curve (x_0, y_0, x_15, y_15, x_30, y_30, x_45, y_45, x_60, y_60):

    sns.set(style='whitegrid')
    plt.title('ROC curve')
    plt.ylabel('Recall')
    plt.ylim((0.0, 1.0))
    plt.xlabel('Precision')
    plt.xlim((0.0, 1.0))

    #plot_prec_rec(x_0, y_0, '-', 'o')
    #plot_prec_rec(x_15, y_15, '-', 'v')
    #plot_prec_rec(x_30, y_30, '-', '*')
    #plot_prec_rec(x_45, y_45, '-', '*')
    plot_prec_rec(x_60, y_60, '-', '*')

    plt.legend (loc=2, borderaxespad=0.)
    plt.savefig('figs_ROC_curves_diff_traces_intervals/roc_curve_error_60.png')
    plt.close()

def plot_prec_rec(x, y, line_type, marker_type):
    for key, x_values in x.iteritems():
        if key == 'not_filtered':
            plt.plot(x_values,y[key], marker = marker_type, linestyle = line_type, color = 'b', label = 'not filtered')
            cont = -1
            for label in trace_intervals:
                cont+=1
                #if label == '30' or label == '45' or label == '2' or label == '3' or label == '4':
                    #continue
                plt.annotate(label, xy=(x_values[cont], y[key][cont]), xytext=(-20, 20),
                         textcoords='offset points', ha='right', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                         arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))

        elif key == 'filtered':
            plt.plot(x_values,y[key], marker = marker_type, linestyle = line_type, color = 'black', label = 'filtered by both interval and blacklist')
        elif key == 'interval':
            plt.plot(x_values,y[key], marker = marker_type, linestyle = line_type, color = 'c', label = 'filtered by interval')
        elif key == 'blist':
            print 'entrou'
            plt.plot(x_values,y[key], marker = marker_type, linestyle = line_type, color = 'r', label = 'filtered by blacklist')
        elif key == '1_sec':
            plt.plot(x_values,y[key], marker = marker_type, linestyle = line_type, color = 'g', label = 'filtered by < 1 second')


def divide_value_per_100(elem_list):

    perc_list = []
    for elem in elem_list:
        perc_list.append(elem/100)

    return perc_list


if __name__ == "__main__":
    plot_sliding_window()
