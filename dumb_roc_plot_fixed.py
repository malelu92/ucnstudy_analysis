import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict

def main_beginning_of_day():

    x = defaultdict(list)
    y = defaultdict(list)

    y_not_filtered_beg = [0.671875, 0.7857142857142857, 0.8450704225352113, 0.8873239436619719, 0.9154929577464789]
    x_not_filtered_beg = [0.7543859649122807, 0.873015873015873, 0.8955223880597015, 0.9, 0.9027777777777778]

    y_not_filtered_end = [0.7323943661971831, 0.8309859154929577, 0.8591549295774648, 0.8732394366197183, 0.9027777777777778]
    x_not_filtered_end = [0.8666666666666667, 0.8805970149253731, 0.8840579710144928, 0.8857142857142857, 0.9027777777777778]

    y_blist_filtered_beg = [0.671875, 0.7857142857142857, 0.8450704225352113, 0.8873239436619719, 0.9154929577464789]
    x_blist_filtered_beg = [0.7543859649122807, 0.873015873015873, 0.8955223880597015, 0.9, 0.9027777777777778]

    y_blist_filtered_end = [0.6901408450704225, 0.7746478873239436, 0.8450704225352113, 0.8591549295774648, 0.9027777777777778]
    x_blist_filtered_end = [0.8596491228070176, 0.873015873015873, 0.8823529411764706, 0.8840579710144928, 0.9027777777777778]

    #obs: no difference when considering only spikes
    y_interval_filtered_beg = [0.6666666666666666, 0.7857142857142857, 0.8450704225352113, 0.8873239436619719, 0.9154929577464789]
    x_interval_filtered_beg = [0.7368421052631579, 0.873015873015873, 0.8955223880597015, 0.9, 0.9027777777777778]

    y_interval_filtered_end = [0.7323943661971831, 0.8309859154929577, 0.8591549295774648, 0.8732394366197183, 0.9027777777777778]
    x_interval_filtered_end = [0.8666666666666667, 0.8805970149253731, 0.8840579710144928, 0.8857142857142857, 0.9027777777777778]

    y_1s_filtered_beg = [0.6612903225806451, 0.782608695652174, 0.8450704225352113, 0.8873239436619719, 0.9154929577464789]
    x_1s_filtered_beg = [0.7192982456140351, 0.8571428571428571, 0.8955223880597015, 0.9, 0.9027777777777778]

    y_1s_filtered_end = [0.7323943661971831, 0.8169014084507042, 0.8450704225352113, 0.8591549295774648, 0.8888888888888888]
    x_1s_filtered_end = [0.8666666666666667, 0.8787878787878788, 0.8823529411764706, 0.8840579710144928, 0.9014084507042254]

    x['not_filtered_beg'] = x_not_filtered_beg
    y['not_filtered_beg'] = y_not_filtered_beg
    x['not_filtered_end'] = x_not_filtered_end
    y['not_filtered_end'] = y_not_filtered_end

    #x['filtered_beg'] = x_filtered_beg
    #y['filtered_beg'] = y_filtered_beg
    #x['filtered_end'] = x_filtered_end
    #y['filtered_end'] = y_filtered_end

    x['blist_beg'] = x_blist_filtered_beg
    y['blist_beg'] = y_blist_filtered_beg
    x['blist_end'] = x_blist_filtered_end
    y['blist_end'] = y_blist_filtered_end

    # both spikes and >1s filter
    x['interval_beg'] = x_interval_filtered_beg
    y['interval_beg'] = y_interval_filtered_beg
    x['interval_end'] = x_interval_filtered_end
    y['interval_end'] = y_interval_filtered_end

    plot_roc_curve(x,y,'end')

def main():

    x = defaultdict(list) #precision
    y = defaultdict(list) #recall

    y_interval_filtered_day = [4.98887679273, 6.37320332592, 6.58257206418, 6.69853741658, 6.78626086682, 7.06631324845, 7.18890519241, 7.32569697543, 7.43629794418]
    x_interval_filtered_day = [53.6368571041, 68.5166652532, 70.7675345603, 72.0142481554, 72.9573403443, 75.9681112713, 77.2847547323, 78.7553429676, 79.9430093119]

    y_blist_filtered_day = [4.08056042032, 5.32336189079, 5.52168630978, 5.64759154952, 5.74209936732, 6.00053643836, 6.12423281425, 6.3003108187, 6.42148277875]
    x_blist_filtered_day = [53.5343917534, 69.8333850771, 72.4350615751, 74.0867225499, 75.3265031564, 78.7167546311, 80.3394390976, 82.6492807617, 84.2371057207]

    y_not_filtered_day = [5.78154336473, 7.43566684022, 7.7119325981, 7.87428409145, 7.99750713936, 8.35660529181, 8.52905444849, 8.734794339, 8.8856281851]
    x_not_filtered_day = [52.8643767041, 67.985170439, 70.5111005323, 71.9954991994, 73.1221419195, 76.4054182715, 77.9810161276, 79.8620928421, 81.2399924989]

    x['blist_day'] = divide_value_per_100(x_blist_filtered_day)
    y['blist_day'] = divide_value_per_100(y_blist_filtered_day)

    x['interval_day'] = divide_value_per_100(x_interval_filtered_day)
    y['interval_day'] = divide_value_per_100(y_interval_filtered_day)

    x['not_filtered_day'] = divide_value_per_100(x_not_filtered_day)
    y['not_filtered_day'] = divide_value_per_100(y_not_filtered_day)

    plot_roc_curve(x,y,'day')

def divide_value_per_100(elem_list):

    perc_list = []
    for elem in elem_list:
        perc_list.append(elem/100)

    return perc_list


def plot_roc_curve (x, y, beg_end):

    sns.set(style='whitegrid')
    plt.title('ROC curve - ' + str(beg_end))
    plt.ylabel('Recall')
    plt.ylim((0.0, 1.0))
    plt.xlabel('Precision')
    plt.xlim((0.0, 1.0))

    for key, x_values in x.iteritems():
        if key == 'not_filtered_' + str(beg_end):
            plt.plot(x_values,y[key], 'b', label = 'not filtered')
        elif key == 'filtered_' + str(beg_end):
            plt.plot(x_values,y[key], 'g', label = 'filtered by both spikes and blcklist')
        elif key == 'interval_' + str(beg_end):
            plt.plot(x_values,y[key], 'c', label = 'filtered by interval')
        elif key == 'blist_' + str(beg_end):
            plt.plot(x_values,y[key], 'r', label = 'filtered by blacklist')

    plt.legend (loc=2, borderaxespad=0.)
    #plt.grid(True)
    plt.savefig('figs_ROC_curves/general-%s.png'%(beg_end))
    plt.close()



if __name__ == "__main__":
    main()





