from toggles import *


if __name__ == '__main__':
    
    print(sys.executable)
    print('Приветствую! Введите код ОКПО проверяемой организации:')
    name_of_check_organization = str(input())



    path_of_bases = 'o:\\РАО\\Арсен\\DataBase'
    
    #date1 = '#??/??/2018#'
    date1 = '#31/12/2017#'
    
    path1 = operation_with_os(path_of_bases, 'oper')
    connect = connect_with_mdb(path1)
    
    ROZ = execute_ROZ_form(connect, date1)
    ROZ = create_dataframe_ROZ(ROZ)

    RAO = execute_RAO_form(connect, date1)
    RAO = create_dataframe_RAO(RAO)

    os.chdir('o:\\РАО\\Арсен\\CheckMotion')
    
    ROZ_RAO = pd.concat([ROZ, RAO], ignore_index = True).fillna(0)
    ROZ_RAO = ROZ_RAO[columns_ROZ_RAO]

     
    ROZ_RAO_main = ROZ_RAO.loc[ROZ_RAO['ОКПО'] == name_of_check_organization]
    ROZ_RAO_contra = ROZ_RAO.loc[ROZ_RAO['ОКПО, поставщика или получателя'] == name_of_check_organization]


    list_main = ROZ_RAO_main['ОКПО, поставщика или получателя'].unique().tolist()
    list_contra = ROZ_RAO_contra['ОКПО'].unique().tolist()

    
    # with  open('out_main.txt', 'w') as out_main:
    #     for i in list_main:
    #         print(i+',', file = out_main)

    # with  open('out_contra.txt', 'w') as out_contra:
    #     for i in list_contra:
    #         print(i+',', file = out_contra)


    """First normalization: here, creating the main table, cut off from it those elements in which:
                            1) use organizations that have not reported to the main counterparty
                            2) use ornaisation, about which the main counterparty has not reported
                            3) use organizations that have mutually reported""" 
   

    list_main_main = first_normalisations('org', list_main, list_contra) # 1) use organizations that have not reported to the main counterparty
    result_main = ROZ_RAO_main.loc[ROZ_RAO_main['ОКПО, поставщика или получателя'].isin(list_main_main)] # 1) use organizations that have not reported to the main counterparty

    list_contra_contra = first_normalisations('contra', list_main, list_contra)# 2) use ornaisation, about which the main counterparty has not reported
    result_contra = ROZ_RAO_contra.loc[ROZ_RAO_contra['ОКПО'].isin(list_contra_contra)]# 2) use ornaisation, about which the main counterparty has not reported

    writer = pd.ExcelWriter('Not_move.xlsx', engine='xlsxwriter')
    result_main.to_excel(writer, sheet_name = 'result_main')
    result_contra.to_excel(writer, sheet_name = 'result_contra')
    # result_general.to_excel(writer, sheet_name = 'result_general')
    writer.save()


    list_general_general = first_normalisations('general', list_main, list_contra) #3) use organizations that have mutually reported""" 
    result_general_main = ROZ_RAO_main.loc[ROZ_RAO_main['ОКПО, поставщика или получателя'].isin(list_general_general)]
    result_general_contra = ROZ_RAO_contra.loc[ROZ_RAO_contra['ОКПО'].isin(list_general_general)]
    result_general = pd.concat([result_general_main, result_general_contra], ignore_index = True).fillna(0)


    """Second normalization"""

    writer = pd.ExcelWriter('Move.xlsx', engine='xlsxwriter')
    list_general_main = result_general['ОКПО, поставщика или получателя'].unique().tolist()
    list_general_main.remove(name_of_check_organization)
    for item in list_general_main:
        main_main_main = result_general.loc[result_general['ОКПО, поставщика или получателя'] == item]
        main_main_main_main = result_general.loc[result_general['ОКПО'] == item]
        united = pd.concat([main_main_main, main_main_main_main], ignore_index = True).fillna(0)
        united.columns = columns_united
        reverse_united = united.groupby(['ОКПО', 'Наим. ЮЛ', 'Форма', 'Док, вид', 'Док, номер', 'Док, дата', 
        'Опер, код', 'Код РАО', 'Статус РАО',], as_index = True)['Кол, куб.м', 'Кол, шт','Активность, Бк', 
                                                    'Сум активность, альфа, Бк', 'Сум активность, бета, Бк',].sum()
        reverse_united.to_excel(writer, sheet_name = item)
    
    writer.save()


    #result = pd.concat([ROZ_RAO_main, ROZ_RAO_contra])

    
    # writer = pd.ExcelWriter('Isotop_move.xlsx', engine='xlsxwriter')
    # result_main.to_excel(writer, sheet_name = 'result_main')
    # result_contra.to_excel(writer, sheet_name = 'result_contra')
    # # result_general.to_excel(writer, sheet_name = 'result_general')
    # writer.save()

