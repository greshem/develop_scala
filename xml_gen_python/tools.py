import  datetime;
from  get_my_info import  options ;

def get_cur_time():
    global  options;
    import time;

    if options.mirco_second:
        return datetime.datetime.now().strftime("%H:%M:%S.%f")
    else:
        return time.strftime("%Y-%m-%d_%H:%M:%S",time.localtime())



def get_cur_time2():
    import datetime
    return datetime.datetime.now().strftime("%H:%M:%S.%f")

def gen_xml_file(input, redis=True):

    print "FFF%s"%(len(input));
    assert( isinstance(input, list)) ;
    if redis==True:
        pass;
    else:
        assert( len(input) == 2048) ;

   
    operationtype=1;
    output="""
    <?xml version="1.0" encoding="UTF-8"?>
    """
    if  operationtype == 1:    
        output+="""
<monitorResult xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
"""
    elif  operationtype == 2:  
        output+=""";
<filterResult xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
"""
    output+="""
<idcId>$idcID</idcId>
""";
    for each in  input:
        output+= each;

    if  operationtype == 1:    
        output+="""
<timeStamp>GatherTime</timeStamp></monitorResult>
"""
    elif  operationtype == 2:  
        output+="""
<timeStamp>GatherTime</timeStamp></filterResult>
""";

    fh = open("output_%s.xml"%get_cur_time(), 'a')
    fh.write("%s\n"%(   output) );
    fh.close();
    return output;

