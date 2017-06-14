import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict


def plot_from_file():

    results_file = open('results_14_june_all_devtraffic.txt','w')

    content = data_file.read().splitlines()
    data_precision = defaultdict(list) 
    data_recall = defaultdict(list)
    list_filter = []

    for line in content:

        if line[0] == '=':
            line = line.split('=========',1)[1]
            line = line.rsplit('============', 1)[0]
            filter_type = line
            if filter_type not in list_filter:
                list_filter.append(filter_type)

        elif line[0] == '*':
            line = line.split('***sliding window size ',1)[1]
            line = line.rsplit(' seconds***', 1)[0]
            sliding_window = line

        elif line[0] == 'p':
            line = line.split('precision ',1)[1]
            precision = line
            data_precision[filter_type].append(precision)

        elif line[0] =='r':
            line = line.split('recall ',1)[1]
            recall = line
            data_recall[filter_type].append(recall)


def plot_roc_curve (data_precision, data_recall, list_filter):

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
        
        plot_prec_rec(x, y, get_line_color(filter))


    plt.legend (loc=2, borderaxespad=0.)
    plt.savefig('figs_ROC_curves_devtraffic/roc_curve_all.png')
    plt.close()

def plot_prec_rec(x, y, line_color):

    if line_color == 'r':
        plt.plot(x_values,y[key], 'o', '-', color = line_color, label = 'not filtered')
            cont = -1
            for label in window_error:
                cont+=1
                #if label == '30' or label == '45' or label == '2' or label == '3' or label == '4':
                    #continue
                plt.annotate(label, xy=(x_values[cont], y[key][cont]), xytext=(-20, 20),
                         textcoords='offset points', ha='right', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                         arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))
    elif line_color == 'g':
        plt.plot(x_values,y[key], 'o', '-', color = line_color, label = 'filtered by both interval and blacklist')
    elif color == 'k':
        plt.plot(x_values,y[key], 'o', '-', color = lone_color, label = 'filtered by interval')
    else:
        plt.plot(x_values,y[key], 'o', '-', color = line_color, label = 'filtered by blacklist')


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
