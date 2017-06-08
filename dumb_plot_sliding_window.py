import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict

window_error = ['0s', '15s', '30', '45', '1m', '2', '3', '4', '5m' ]

def plot_sliding_window():

    x_1 = defaultdict(list)
    y_1 = defaultdict(list)

    x_5 = defaultdict(list)
    y_5 = defaultdict(list)

    x_10 = defaultdict(list)
    y_10 = defaultdict(list)


    #x_1 and y_1 ==================

    x_1_not_filtered = [53.1042275015, 58.7892357831, 59.6709544112, 60.6764540322, 61.466280792, 63.9476044683, 66.2540827409, 68.1703939588, 69.8305648798]
    y_1_not_filtered = [7.23978098612, 40.3026547894, 51.7485371377, 59.1477592376, 64.2964155482, 74.4621995301, 78.2404177502, 80.1842799778, 81.368329952]

    #x_5 and y_5 ==================

    x_5_not_filtered = [59.0493069516, 58.9246309246, 59.8610441767, 60.8062030639, 61.5819818905, 64.0505711269, 66.3396162741, 68.2459364632, 69.8963624713]
    y_5_not_filtered = [19.2547548195, 42.6370710206, 53.1834270545, 60.1008987153, 64.9867658391, 74.6768027909, 78.3436093398, 80.2439339257, 81.4026302697]

    #x_10 and y_10 ==================

    x_10_not_filtered = [58.9529818236, 59.0563672686, 60.055883363, 60.9296017754, 61.7045554288, 64.1604566355, 66.4295691628, 68.3229872891, 69.9627956171]
    y_10_not_filtered = [25.7737205904, 44.7579747362, 54.5476457161, 61.0106718609, 65.6658660262, 74.8935802682, 78.4473379427, 80.3010265836, 81.4367636101]

    x_10_filtered = [62.8919860627, 63.4334011083, 64.7411478174, 66.627390804, 68.0332575966, 72.1477049114, 74.9942033638, 76.9511675892, 78.5199217795]
    y_10_filtered = [12.4468451902, 23.6176876605, 29.3970027364, 33.3529944309, 36.3597544702, 44.3335348625, 49.1898594977, 52.663961985, 55.5050379602]

    x_1['blist'] = divide_value_per_100(x_1_blist_filtered)
    y_1['blist'] = divide_value_per_100(y_1_blist_filtered)

    x_1['interval'] = divide_value_per_100(x_1_interval_filtered)
    y_1['interval'] = divide_value_per_100(y_1_interval_filtered)

    x_1['not_filtered'] = divide_value_per_100(x_1_not_filtered)
    y_1['not_filtered'] = divide_value_per_100(y_1_not_filtered)

    x_1['filtered'] = divide_value_per_100(x_1_filtered)
    y_1['filtered'] = divide_value_per_100(y_1_filtered)


    x_5['blist'] = divide_value_per_100(x_5_blist_filtered)
    y_5['blist'] = divide_value_per_100(y_5_blist_filtered)

    x_5['interval'] = divide_value_per_100(x_5_interval_filtered)
    y_5['interval'] = divide_value_per_100(y_5_interval_filtered)

    x_5['not_filtered'] = divide_value_per_100(x_5_not_filtered)
    y_5['not_filtered'] = divide_value_per_100(y_5_not_filtered)

    x_5['filtered'] = divide_value_per_100(x_5_filtered)
    y_5['filtered'] = divide_value_per_100(y_5_filtered)


    x_10['blist'] = divide_value_per_100(x_10_blist_filtered)
    y_10['blist'] = divide_value_per_100(y_10_blist_filtered)

    x_10['interval'] = divide_value_per_100(x_10_interval_filtered)
    y_10['interval'] = divide_value_per_100(y_10_interval_filtered)

    x_10['not_filtered'] = divide_value_per_100(x_10_not_filtered)
    y_10['not_filtered'] = divide_value_per_100(y_10_not_filtered)

    x_10['filtered'] = divide_value_per_100(x_10_filtered)
    y_10['filtered'] = divide_value_per_100(y_10_filtered)


def plot_roc_curve (x_1, y_1, x_5, y_5, x_10, y_10):

    sns.set(style='whitegrid')
    plt.title('ROC curve')
    plt.ylabel('Recall')
    plt.ylim((0.0, 1.0))
    plt.xlabel('Precision')
    plt.xlim((0.0, 1.0))

    plot_prec_rec(x_1, y_1, '-')
    plot_prec_rec(x_5, y_5, '--')
    plot_prec_rec(x_10, y_10, '-.')

    plt.legend (loc=2, borderaxespad=0.)
    plt.savefig('figs_ROC_curves/general-%s.png'%(beg_end))
    plt.close()

def plot_prec_rec(x, y, line_type)
    for key, x_values in x.iteritems():
        if key == 'not_filtered':
            plt.plot(x_values,y[key], marker = 'o', linestyle = line_type, color = 'b', label = 'not filtered')
            for label in window_error:
            cont+=1
                if label == '30' or label == '45' or label == '2' or label == '3' or label == '4':
                    continue
                plt.annotate(label, xy=(x_values[cont], y[key][cont]), xytext=(-20, 20),
                         textcoords='offset points', ha='right', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                         arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))

        elif key == 'filtered':
            plt.plot(x_values,y[key], marker = 'o', linestyle = line_type, color = 'black', label = 'filtered by both interval and blacklist')
        elif key == 'interval':
            plt.plot(x_values,y[key], marker = 'o', linestyle = line_type, color = 'c', label = 'filtered by interval')
        elif key == 'blist':
            plt.plot(x_values,y[key], marker = 'o', linestyle = line_type, color = 'r', label = 'filtered by blacklist')
        elif key == '1_sec':
            plt.plot(x_values,y[key], marker = 'o', linestyle = line_type, color = 'g', label = 'filtered by < 1 second')


if __name__ == "__main__":
    plot_sliding_window()
