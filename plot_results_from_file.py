import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict

def plot_from_file():

    results_file = open('results_14_june_all_devtraffic.txt','r')

    content = results_file.read().splitlines()
    data_precision = defaultdict(list) 
    data_recall = defaultdict(list)
    list_filter = []
    list_sliding_window = []

    for line in content:
        if line[2] == '=':
            line = line.split('=========',1)[1]
            line = line.rsplit('============', 1)[0]
            filter_type = line
            if filter_type not in list_filter:
                list_filter.append(filter_type)

        elif line[2] == '*':
            line = line.split('***sliding window size ',1)[1]
            line = line.rsplit(' seconds***', 1)[0]
            sliding_window = int(line)
            if sliding_window != 1:
                sliding_window -= 1
            if sliding_window not in list_sliding_window:
                list_sliding_window.append(sliding_window)


        elif line[0] == 'p':
            line = line.split('precision ',1)[1]
            precision = float(line)
            data_precision[filter_type].append(precision)

        elif line[0] =='r':
            line = line.split('recall ',1)[1]
            recall = float(line)
            data_recall[filter_type].append(recall)

    plot_roc_curve (data_precision, data_recall, list_filter, list_sliding_window)


def plot_roc_curve (data_precision, data_recall, list_filter, list_sliding_window):

    sns.set(style='whitegrid')
    plt.title('ROC curve')
    plt.ylabel('Recall')
    plt.ylim((0.0, 1.0))
    plt.xlabel('Precision')
    plt.xlim((0.0, 1.0))
    
    for filter in list_filter:
        x = []
        y = []
        for elem in data_precision[filter]:
            x.append(elem)

        for elem in data_recall[filter]:
            y.append(elem)

        x = divide_value_per_100(x)
        y = divide_value_per_100(y)
        plot_prec_rec(x, y, get_line_color(filter), list_sliding_window)

    plt.legend (loc=2, borderaxespad=0.)
    plt.savefig('figs_ROC_curves_devtraffic/roc_curve_all.png')
    plt.close()

def divide_value_per_100(elem_list):

    perc_list = []
    for elem in elem_list:
        perc_list.append(elem/100)

    return perc_list

def plot_prec_rec(x, y, line_color, sliding_window):

    if line_color == 'r':
        plt.plot(x, y, '-o', color = line_color, label = 'not filtered')
        cont = -1
        for label in sliding_window:
            cont+=1
            #if label == '30' or label == '45' or label == '2' or label == '3' or label == '4':
            #continue
            plt.annotate(label, xy=(x[cont], y[cont]), xytext=(-20, 20),
                         textcoords='offset points', ha='right', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                         arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))
    elif line_color == 'g':
        plt.plot(x, y, '-o', color = line_color, label = 'filtered by both interval and blacklist')
    elif line_color == 'k':
        plt.plot(x, y, '-o', color = line_color, label = 'filtered by interval')
    else:
        plt.plot(x, y, '-o', color = line_color, label = 'filtered by blacklist')


def get_line_color(filter):
    
    if filter == 'filtered':
        return 'g'
    elif filter == 'not_filtered':
        return 'r'
    elif filter == 'blist_filtered':
        return 'b'
    else:
        return 'k'

if __name__ == "__main__":
    plot_from_file()
