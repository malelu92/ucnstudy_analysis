import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict

window_error = ['0s', '15s', '30', '45', '1m', '2', '3', '4', '5m' ]

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

def final_algorithm():

    #sliding window 1s

    1y_filtered_day = [4.29783754366, 22.3979945575, 29.5019610659, 34.2947966095, 37.8947853996]
    1x_filtered_day = [61.7436743674, 69.8844959528, 70.4566626962, 71.5533786023, 72.5247112913]

    1y_not_filtered_day = [13.8263744739, 49.2963670093, 60.7949837079, 67.8424520269, 72.4749791086]
    1x_not_filtered_day = [50.121447054, 51.6531080176, 51.5714335102, 52.0024578547, 52.4209175489]

    #sliding window 5s
    5y_filtered_day = [9.08747721947, 23.5832656126, 30.2522708227, 34.8436854428, 38.3311557611]
    5x_filtered_day = [64.5552062142, 69.9389871873, 70.6314937255, 71.7025336785, 72.6487861194]

    5y_not_filtered_day = [24.9661546472, 51.2215444436, 61.9070094493, 68.5295872928%, 72.9492566828]
    5x_not_filtered_day = [50.7327266956, 51.5866355866, 51.6216606962, 52.0444308087, 52.4693631342]

    5y_blist_filtered_day = [18.4118719084, 39.0215300423, 47.7503684368, 53.6418307855, 57.8348458632]
    5x_blist_filtered_day = [56.7121090617, 62.1573350374, 62.3614657894, 62.6550988236, 62.9013365082]

    5y_interval_filtered_day = [13.5303306431, 34.2999393085, 43.9976111812, 50.6293898912, 55.6764010029]
    5x_interval_filtered_day = [54.5158921641, 54.2681835197, 53.7073182203, 54.4702918951, 55.0576609137]


    #sliding window 10s
    10y_not_filtered_day = [33.2958523932, 53.4112823011, 63.2607076706, 69.3417213713, 73.5715915673]
    10x_not_filtered_day = [48.9296980622, 51.5335500803, 51.7139645968, 52.0917808219, 52.5535089589]

    10y_filtered_day = [13.1481434157, 24.929460244, 31.1138268823, 35.4777534646%, 38.8685074339]
    10x_filtered_day = [64.2081617372, 70.0370687197, 70.7973061047, 71.8676122931, 72.8148635317]

    10y_blist_filtered_day = [24.6505328256, 40.7344254309, 48.9144341118, 54.3927789934, 58.4616194773]
    10x_blist_filtered_day = [56.5018744142, 62.1269349845, 62.4338882254, 62.6712328767, 62.9596022609]

    10y_interval_filtered_day = [19.5267179474, 36.2056715994, 45.2767858848, 51.5116703136, 56.4552651442]
    10x_interval_filtered_day = [52.3463725423, 54.0976645435, 53.8335792939, 54.5684592799, 55.1730053351]

    sliding_window = [1, 5, 10]

    for size in sliding_window:
        x['1_sec_day'] = divide_value_per_100(x_1sec_filtered_day)
        y['1_sec_day'] = divide_value_per_100(y_1sec_filtered_day)

        x['blist_day'] = divide_value_per_100(x_blist_filtered_day)
        y['blist_day'] = divide_value_per_100(y_blist_filtered_day)

        x['interval_day'] = divide_value_per_100(x_interval_filtered_day)
        y['interval_day'] = divide_value_per_100(y_interval_filtered_day)

        x['not_filtered_day'] = divide_value_per_100(x_not_filtered_day)
        y['not_filtered_day'] = divide_value_per_100(y_not_filtered_day)

        x['filtered_day'] = divide_value_per_100(x_filtered_day)
        y['filtered_day'] = divide_value_per_100(y_filtered_day)

def first_test():

    x = defaultdict(list) #precision
    y = defaultdict(list) #recall

    y_1sec_filtered_day = [4.98887679273, 6.37320332592, 6.58257206418, 6.69853741658, 6.78626086682, 7.06631324845, 7.18890519241, 7.32569697543, 7.43629794418]
    x_1sec_filtered_day = [53.6368571041, 68.5166652532, 70.7675345603, 72.0142481554, 72.9573403443, 75.9681112713, 77.2847547323, 78.7553429676, 79.9430093119]

    y_blist_filtered_day = [4.08056042032, 5.32336189079, 5.52168630978, 5.64759154952, 5.74209936732, 6.00053643836, 6.12423281425, 6.3003108187, 6.42148277875]
    x_blist_filtered_day = [53.5343917534, 69.8333850771, 72.4350615751, 74.0867225499, 75.3265031564, 78.7167546311, 80.3394390976, 82.6492807617, 84.2371057207]

    y_not_filtered_day = [5.78154336473, 7.43566684022, 7.7119325981, 7.87428409145, 7.99750713936, 8.35660529181, 8.52905444849, 8.734794339, 8.8856281851]
    x_not_filtered_day = [52.8643767041, 67.985170439, 70.5111005323, 71.9954991994, 73.1221419195, 76.4054182715, 77.9810161276, 79.8620928421, 81.2399924989]

    y_interval_filtered_day = [1.43260598602, 1.87990091668, 1.93117811331, 1.9573689276, 1.96999100677, 2.01606159575, 2.0335747306, 2.05108786545, 2.06954765624]
    x_interval_filtered_day = [60.368326574, 79.2115410185, 81.3721579577, 82.4757346098, 83.0075787794, 84.9488099987, 85.6867437841, 86.4246775695, 87.2024996676]

    y_filtered_day = [1.06325239425, 1.4100440195, 1.45153910478, 1.4711033275, 1.47930767896, 1.51449172465, 1.52774490778, 1.5411558669, 1.55551348196]
    x_filtered_day = [63.022538109, 83.5702262951, 86.02954928, 87.1890779877, 87.6753319618, 89.7606134281, 90.5461006172, 91.3409388442, 92.191883299]

    x['1_sec_day'] = divide_value_per_100(x_1sec_filtered_day)
    y['1_sec_day'] = divide_value_per_100(y_1sec_filtered_day)

    x['blist_day'] = divide_value_per_100(x_blist_filtered_day)
    y['blist_day'] = divide_value_per_100(y_blist_filtered_day)

    x['interval_day'] = divide_value_per_100(x_interval_filtered_day)
    y['interval_day'] = divide_value_per_100(y_interval_filtered_day)

    x['not_filtered_day'] = divide_value_per_100(x_not_filtered_day)
    y['not_filtered_day'] = divide_value_per_100(y_not_filtered_day)

    x['filtered_day'] = divide_value_per_100(x_filtered_day)
    y['filtered_day'] = divide_value_per_100(y_filtered_day)

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
            plt.plot(x_values,y[key], marker = 'o', linestyle = '-', color = 'b', label = 'not filtered')
            cont = -1
            for label in window_error:
                cont+=1
                if label == '30' or label == '45' or label == '2' or label == '3' or label == '4':
                    continue
                plt.annotate(label, xy=(x_values[cont], y[key][cont]), xytext=(-20, 20),
                         textcoords='offset points', ha='right', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                         arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))
        elif key == 'filtered_' + str(beg_end):
            plt.plot(x_values,y[key], marker = 'o', linestyle = '-', color = 'black', label = 'filtered by both interval and blacklist')
        elif key == 'interval_' + str(beg_end):
            plt.plot(x_values,y[key], marker = 'o', linestyle = '-', color = 'c', label = 'filtered by interval')
        elif key == 'blist_' + str(beg_end):
            plt.plot(x_values,y[key], marker = 'o', linestyle = '-', color = 'r', label = 'filtered by blacklist')
        elif key == '1_sec_' + str(beg_end):
            plt.plot(x_values,y[key], marker = 'o', linestyle = '-', color = 'g', label = 'filtered by < 1 second')

    plt.legend (loc=2, borderaxespad=0.)
    #plt.grid(True)
    plt.savefig('figs_ROC_curves/general-%s.png'%(beg_end))
    plt.close()



if __name__ == "__main__":
    main()





