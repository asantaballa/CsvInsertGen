import datetime
import sys
import csv
from tabulate import tabulate
# import random
# import string

class ColInfo:
    name = ""
    maxlen = 0

multiplierforsize = 1.1    

print("Starting")

if len(sys.argv) <= 3:
    print('No parms!')
    exit()
else:
    fileinname = sys.argv[1]
    fileoutname = sys.argv[2]
    tblname =  sys.argv[3]

outfile = open(fileoutname, "w")


# -- Go through once to get stats
print("gathering stats")
with open(fileinname, newline="\n") as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
 
    # Get first row for column names
    cols = []
    for row in csvreader: 
        colnames = row;    
        sqlcolnames = ", ".join(colnames)

        for col in colnames:
            newcol = ColInfo()
            newcol.name = col
            newcol.maxlen = 0
            cols.append(newcol)

        break    

    # loop through rest of rows of csv
    cnt = 0
    for row in csvreader:

        cnt = cnt + 1

        colvaluesquotedlist = []
        i = -1
        for col in row:
            i+=1
            colinfo = cols[i]

            if (len(col) > colinfo.maxlen):
                colinfo.maxlen = len(col)

outfile.write("-- == CsvInsertGen.py == -- \n")
outfile.write("-- Created       = {datetime}\n".format(datetime=datetime.datetime.now()))
outfile.write("-- fileinname    = {fileinname}\n".format(fileinname=fileinname))
outfile.write("-- fileoutname   = {fileoutname}\n".format(fileoutname=fileoutname))
outfile.write("-- tblname       = {tblname}\n".format(tblname=tblname))
outfile.write("-- Total rows    = {cnt:,} (+ 1 header)\n".format(cnt=cnt))
outfile.write("\n")

coltab = []
for colinfo in cols:
    coltab.append([colinfo.name, colinfo.maxlen])

# -- Generate the DDL

outfile.write("SET NOCOUNT ON\n")
outfile.write("\n")

outfile.write("IF EXISTS(Select * from sys.objects Where Name = '{tblname}' and Type = 'U')\n".format(tblname=tblname))
outfile.write("BEGIN\n")
outfile.write("    DROP TABLE {tblname}\n".format(tblname=tblname))
outfile.write("END\n")
outfile.write("GO\n")
outfile.write("\n")

outfile.write("CREATE TABLE {tblname}\n".format(tblname=tblname))
outfile.write("(\n")
outfile.write("  _ID INT NOT NULL IDENTITY (1,1)\n")

for colinfo in cols:
    # coltab.append([colinfo.name, colinfo.maxlen])
    collentouse = int(max([50, colinfo.maxlen * multiplierforsize]))

    outfile.write(", {colname} NVARCHAR({collentouse}) NULL".format(colname=colinfo.name, collentouse=collentouse))
    outfile.write("\n".format(colname=colinfo.name))

outfile.write(")\n")
outfile.write("GO\n")
outfile.write("\n")

# -- Now go through to do actual work

print("processing file")
with open(fileinname, newline="\n") as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
 
    # Get first row to skip it
    for row in csvreader: 
        # -- Do nothing with first row
        break    

    # Loop through rest of rows of csv
    cnt = 0
    for row in csvreader:

        cnt = cnt + 1

        colvaluesquotedlist = []
        i = -1
        for col in row:
            # col = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            colvaluequoted = "\'" + col.replace("\'","\'\'") + "\'"
            colvaluesquotedlist.append(colvaluequoted)

        outfile.write("INSERT {tblname} ({sqlcolnames})\n".format(tblname=tblname, sqlcolnames=sqlcolnames))
        colvaluesquoted = ", ".join(colvaluesquotedlist)
        outfile.write("VALUES ({colvaluesquoted})\n".format(colvaluesquoted=colvaluesquoted))
        outfile.write("\n")    

        if ((cnt % 1000) == 0):
            outfile.write("GO\n")    
            outfile.write("\n")    
            outfile.write("print 'Finished #{cnt}'\n".format(cnt=cnt))    
            outfile.write("\n")    


outfile.write("SELECT Cnt = COUNT(*) FROM {tblname}\n".format(tblname=tblname))
outfile.write("GO\n")    
outfile.write("\n")

outfile.close()

print()
print("File In:", fileinname)    
print("File Out:", fileoutname)    
print("Rows:", cnt)    
print()    
print(tabulate(coltab, headers=["name","maxlen"]))
print()
