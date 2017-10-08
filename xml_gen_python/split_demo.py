data=range(0,10248);
count=len(data)/1024;

for each in range(0, count):
    print  ("%s-> %s", each*1024, (each+1)*1024-1); 

for each in data[(count)*1024:]:
    print each;
