try:
    input_data=eval(input('Enter u r data:-'))
    if type(input_data) == int:
        print(input_data,'Entered input is int type only')
    elif type(input_data) == str:
        if input_data.isdigit():
            print(input_data,'Entered input is int in string')
        else:
            print(input_data,'Entered input is sting only')
except NameError:
    print('its string')