# encoding=utf-8
# 君子爱财 取之YOODO!

# 单位: 百万
AMOUNT_UNIT_M = 1000000
# 单位: 十亿
AMOUNT_UNIT_B = 1000000000

def str_to_digit(s):
    ''' 将字符串转为数字
        @Param s: 数值串(如: 3,111.63)
    '''
    s = s.replace(",", "")
    if len(s) == 0:
        return float(0)
    return float(s)

def str_with_unit_to_digit(s, unit):
    ''' 将金额转为数字
        @Param val: 数值(如: 3,111.63)
        @Param unit: 单位(M:百万; B:十亿)
    '''

    digit = str_to_digit(s)

    if unit.upper() == "M":
        return digit * AMOUNT_UNIT_M
    elif unit.upper() == "B":
        return digit * AMOUNT_UNIT_B
    elif unit.upper() == "":
        return digit
    return 0


def digit(d, unit):
    ''' 将金额转为数字
        @Param val: 数值
        @Param unit: 单位(M:百万; B:十亿)
    '''

    digit = float(val.replace(",", ""))

    if unit.upper() == "M":
        return digit * AMOUNT_UNIT_M
    elif unit.upper() == "B":
        return digit * AMOUNT_UNIT_B
    elif unit.upper() == "":
        return digit
    return None

