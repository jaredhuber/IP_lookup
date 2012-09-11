import sys, json, os, requests, time, re
from time import strftime

#config
ip_file = '0806_ips.txt'
batch_size = 1000 #the number of records polled prior to writing to a file
output_file = strftime("%Y-%m-%d %H:%M:%S") + '_ip_responses.txt'
api_key = sys.argv[1]
api_base = 'http://api.demandbase.com/api/v2/ip.json?key='
wait_seconds = 0.0
delimiter = '\t'
field_mapper = {    
0 : {'a':'ip', 'h':'IP Address'},
1 : {'a':'demandbase_sid', 'h': 'Demandbase ID'},
2 : {'a':'duns_num', 'h':'DUNS Number'},
3 : {'a':'company_name', 'h':'Company Name'},
4 : {'a':'city', 'h':'City'},
5 : {'a':'state', 'h':'State'},
6 : {'a':'country','h':'Country Code'},
7 : {'a':'information_level','h':'Detailed or Basic'},
8 : {'a':'audience','h':'Audience'},
9 : {'a':'marketing_alias','h':'Marketing Alias'},
10: {'a':'audience_segment','h':'Audience Sub Segments'},
11: {'a':'forbes_2000','h':'Forbes 2000'},
12: {'a':'fortune_1000','h':'Fortune 1000'},
13: {'a':'industry','h':'Primary Industry'},
14: {'a':'sub_industry','h':'Sub Industry'},
15: {'a':'employee_count','h':'Employee Count'},
16: {'a':'employee_range','h':'Employee Band'},
17: {'a':'annual_sales','h':'Annual Sales'},
18: {'a':'revenue_range','h':'Revenue Band'},
19: {'a':'phone','h':'Phone Number'},
20: {'a':'street_address','h':'Street Address'},
21: {'a':'zip','h':'Zip'},
22:{'a':'country_name','h':'Country Name'},
23:{'a':'web_site','h':'Website'},
24:{'a':'stock_ticker','h':'Stock Ticker'},
25:{'a':'primary_sic','h':'Primary SIC Code'},
26:{'a':'latitude','h':'Latitude'},
27:{'a':'longitude','h':'Longitude'},
28:{'a':'registry_area_code','h':'Public Area Code'},
29:{'a':'registry_city','h':'Public City'},
30:{'a':'registry_company_name','h':'Public Company Name'},
31:{'a':'registry_country_code','h':'Public Country Code'},
32:{'a':'registry_latitude','h':'Public Latitude'},
33:{'a':'registry_longitude','h':'Public Longitude'},
34:{'a':'registry_state','h':'Public State Classification'},
35:{'a':'registry_zip_code','h':'Public Zip Code'},
36:{'a':'registry_dma_code','h':'Public DMA Code'},
37:{'a':'registry_country','h':'Public Country Name'},
38:{'a':'isp','h':'ISP Flag'},
39:{'a':'date_stamp','h':'Timestamp'},
40:{'a':'domestichq_sid','h':'Demandbase Dom. HQ'},
41:{'a':'hq_sid','h':'Demandbase HQ ID'},
42:{'a':'worldhq_sid','h':'Company Global HQ ID'},
43:{'a':'domduns_num','h':'DnB ID: Domestic HQ'},
44:{'a':'hqduns_num','h':'DnB ID: HQ'},
45:{'a':'gltduns_num','h':'DnB ID: Global HQ'},
46:{'a':'registry-country-code3','h':'Public Country Code 3'},
47:{'a':'account_watch','h':'Account Watch Tag(s)'}
}

def isIP(ip_address):
    return bool(re.search('((2[0-5]|1[0-9]|[0-9])?[0-9]\.){3}((2[0-5]|1[0-9]|[0-9])?[0-9])',ip_address))

def makeURL(ip, base_url, key):
    return base_url + key + '&query=' + ip # returns full URL for call

def callRecord(url):
    #print url
    r = requests.get(url)
    return r.json # returns the json response for one IP address

def getThisField(obj, field_name):
    if field_name == 'date_stamp': 
        this_field = strftime("%Y-%m-%d")
    else:
        this_field = obj.get(field_name,'')
    return this_field

def makeOutputString(obj, mapper, delimiter):
    outputString = ''
    #print outputString
    for i in mapper:
        field_name = mapper[i]['a']
        #print field_name
        this_field = getThisField(obj, field_name)
        #print this_field
        outputString = outputString + str(this_field) + delimiter
        #print outputString
    return outputString
	
def makeOutputHeader(file, mapper, delimiter):
    header = ''
    for i in mapper:
        header = header + str(mapper[i]['h']) + delimiter
    return header        

def saveToFile(file_name, added_string):
    w = open(file_name, "a")
    try:
        w.write(added_string) # + '\n')
    finally:
        w.close()
		
def main():       
    f = open(ip_file, 'r')
    w = open(output_file, 'w')
    saveToFile(output_file,makeOutputHeader(output_file,field_mapper, delimiter))
    ip_list = f.readlines()
    counter = 0
    output_line = ''
    for ip_line in ip_list:
        this_ip = ip_line[:ip_line.find('\n')] #Read IP
        print this_ip
        if isIP(this_ip):
            this_url = makeURL(this_ip, api_base, api_key) #Create URL to call
            api_response = callRecord(this_url)  #call URL / get response
            #print api_response
            output_line = output_line + makeOutputString(api_response, field_mapper, delimiter) + '\n' #map response fields to output fields / create output string 
             #+ str(api_response) #
        else:
            output_line = output_line + this_ip + ' is not a valid IP ################################################################' +'\n'
        #print output_line    
        counter = counter +1
        #print 'counter' , counter
        if counter > batch_size:
            saveToFile(output_file, output_line + '\n') #write output string to output file
            print 'saved a batch of ' + batch_size + 'to a file'
            counter = 0
            output_line = ''
        time.sleep(wait_seconds) #wait a second
    f.close()

if __name__ == "__main__":
    main()
