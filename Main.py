import requests
import gzip
import mysql.connector
import csv
import matplotlib.pyplot as plt

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="python_data"
)  # δεδομένα για να συνδεθώ με την βάση MySQL

url = ['https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_nim.tsv.gz',
       'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_ninrmw.tsv.gz',
       'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_arm.tsv.gz',
       'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_arnrmw.tsv.gz']
#  URLs των αρχείων
#  1ο αρχείο: Ελλάδα(NAT,NR,I551,EL) Ισπανία(NAT,NR,I551,ES) 2021M02
#  2ο αρχείο: Ελλάδα(NR,I551-I553,EU27_2007,EL) Ισπανία(NR,I551-I553,EU27_2007,ES) 2011M12
#  3ο αρχείο: Ελλάδα(NAT,NR,I551-I553,EL) Ισπανία(NAT,NR,I551-I553,ES) 2021M02
#  4ο αρχείο: Ελλάδα(NR,I551-I553,EU27_2007,EL) Ισπανία(NR,I551-I553,EU27_2007,ES) 2011M12
start_gr = ['NAT,NR,I551,EL',
            'NR,I551-I553,EU27_2007,EL',
            'NAT,NR,I551-I553,EL',
            'NR,I551-I553,EU27_2007,EL']

end_gr = ['NAT,NR,I551,ES',
          'NR,I551-I553,EU27_2007,ES',
          'NAT,NR,I551-I553,ES',
          'NR,I551-I553,EU27_2007,ES']
end_es = ['NAT,NR,I551,EU27_2007',
          'NR,I551-I553,EU27_2007,EU25',
          'NAT,NR,I551-I553,EU27_2007',
          'NR,I551-I553,EU27_2007,FI']
# Οι παραπάνω πίνακες δείχνουν τα σημεία που αρχίζουν και τελειώνουν τα δεδομένα των χωρών
filename = ['', '', '', '']
i = 0
for u in url:
    r = requests.get(u, allow_redirects=True)  # κατεβάζω το αρχείο
    filename[i] = u.rsplit('/', 1)[-1]  # ονομάζω το αρχείο σύμφωνα με το link
    open(filename[i], 'wb').write(r.content)  # αποθηκεύω το αρχείο
    i = i + 1
i = 0
file_content_str = ['', '', '', '']
for a in filename:
    f = gzip.open(a, 'rb')  # ανοίγω το gz αρχείο
    file_content = f.read()  # διαβάζω τα περιεχόμενα του και τα αποθηκεύω σε μορφή bytes
    f.close()

    file_content_str[i] = file_content.decode()  # μετατρέπω τα bytes σε string
    i = i + 1
date_final = '9999M99'
i = 0
for b in file_content_str:
    date = b.split("\t", 1)[1]
    date = date.split("\t", 1)[0]
    if date_final > date:
        date_final = date  # βρίσκω τη μικρότερη ημερομηνία των 2 μηνών
i = 0
data_gr = ['', '', '', '']
data_es = ['', '', '', '']
for b in file_content_str:
    str1 = b.split(start_gr[i], 1)[1]  # βρίσκω που ξεκινάνε τα δεδομένα της Ελλάδας
    data_gr = str1.split(end_gr[i], 1)[0]  # βρίσκω που τελειώνουν τα δεδομένα της Ελλάδας
    str2 = b.split(end_gr[i], 1)[1]  # βρίσκω που ξεκινάνε τα δεδομένα της Ισπανίας
    data_es = str2.split(end_es[i], 1)[0]  # βρίσκω που τελειώνουν τα δεδομένα της Ισπανίας
    res_gr = [int(s) for s in data_gr.split() if s.isdigit()]  # αποθηκεύω τα δεδομένα σε έναν int πίνακα
    # χρησιμοποιώ list comprehension
    # μετά περνάω τα δεδομένα σε ξεχωριστούς πίνακες για να τα χρησιμοποιήσω παρακάτω
    if i == 0:
        nsatae_gr = res_gr[109:]  # κόβω τα δεδομένα για να βρω αντιστοιχία στους μήνες (2011/12 - 2008/01)
    elif i == 1:
        nsbnratae_gr = res_gr
    elif i == 2:
        aatae_gr = res_gr[106:]
    else:
        aonratae_gr = res_gr

    res_es = [int(s) for s in data_es.split() if s.isdigit()]

    if i == 0:
        nsatae_es = res_es[110:]
    elif i == 1:
        nsbnratae_es = res_es
    elif i == 2:
        aatae_es = res_es[110:]
    else:
        aonratae_es = res_es
    i = i + 1

mycursor = mydb.cursor()

tables = ['nsatae_gr',
          'nsbnratae_gr',
          'aatae_gr',
          'aonratae_gr',
          'nsatae_es',
          'nsbnratae_es',
          'aatae_es',
          'aonratae_es']  # ονόματα πινάκων στην βάση
j = 0

csv_names = ['Nights spent at tourist accommodation establishments of Greece.csv',
             'Nights spent by non-residents at tourist accommodation establishments of Greece.csv',
             'Arrivals at tourist accommodation establishments of Greece.csv',
             'Arrivals of non-residents at tourist accommodation establishments of Greece.csv',
             'Nights spent at tourist accommodation establishments of Spain.csv',
             'Nights spent by non-residents at tourist accommodation establishments of Spain.csv',
             'Arrivals at tourist accommodation establishments of Spain.csv',
             'Arrivals of non-residents at tourist accommodation establishments of Spain.csv',
             ]   # ονόματα αρχείων csv που θα δημιουργηθούν

for t in tables:
    sql_str = "INSERT IGNORE INTO {} (date, number) VALUES(%s, %s)".format(t)  # εντολή εισαγωγής SQL
    # περνάω τα δεδομένα μου σε κάθε κύκλο στον πίνακα res
    if j == 0:
        res = nsatae_gr
    elif j == 1:
        res = nsbnratae_gr
    elif j == 2:
        res = aatae_gr
    elif j == 3:
        res = aonratae_gr
    elif j == 4:
        res = nsatae_es
    elif j == 5:
        res = nsbnratae_es
    elif j == 6:
        res = aatae_es
    else:
        res = aonratae_es
    month_str = date_final.split("M", 1)[1]
    year_str = date_final.split("M", 1)[0]
    month = int(month_str)
    year = int(year_str)
    # κάνω την ημερομηνία απο String τύπου 2011M02 σε ξεχωριστούς ακέραιους
    for i in range(48):
        if month == 0:  # αν ο μετρητής του μήνα μηδενιστεί
            month = 12  # τότε ξεκίνα πάλι από το 12 μήνα
            year = year - 1  # και άλλαξε χρονιά
        date_str = "{}-{}-01".format(year, month)  # φτιάχνω το format DATE της MySQL
        val = (date_str, res[i])  # περνάω τα δεδομένα προς ανέβασμα
        mycursor.execute(sql_str, val)  # εκτελώ την εντολή εισαγωγής
        mydb.commit()  # ανανεώνω την βάση
        month = month - 1

    data_sql = "SELECT YEAR(date), MONTH(date), number FROM {} ORDER BY date".format(t)
    # με την παραπάνω εντολή πέρνω τα στοιχεία που με ενδιαφέρουν
    mycursor.execute(data_sql)
    results = mycursor.fetchall()  # τα αποθηκεύω όλα στον πίνακα results
    with open(csv_names[j], 'w', newline='', encoding='UTF8') as c_file:  # ανοίγουμε το αρχείο
        c = csv.writer(c_file, dialect='excel-tab')  # ορίζουμε τον writer
        #  dialect='excel-tab' για να ανοίγει και με εξελ
        c.writerow(["Date", "Number"])
        w = 0
        date_res = [0 for j in range(48)]
        num_res = [0 for j in range(48)]
        for x in results:
            date_res[w] = "{}-{}".format(x[0], x[1])
            num_res[w] = x[2]
            c.writerow([date_res[w], num_res[w]])
            w = w + 1
    if j == 0:
        nsatae_gr = num_res
    elif j == 1:
        nsbnratae_gr = num_res
    elif j == 2:
        aatae_gr = num_res
    elif j == 3:
        aonratae_gr = num_res
    elif j == 4:
        nsatae_es = num_res
    elif j == 5:
        nsbnratae_es = num_res
    elif j == 6:
        aatae_es = num_res
    else:
        aonratae_es = num_res
    j = j + 1

f1 = plt.figure(1)
plt.plot(date_res, nsatae_gr, label="Greece")
plt.plot(date_res, nsatae_es, label="Spain")
plt.title("Nights spent at tourist accommodation establishments")
plt.xlabel("Dates (Year - Month)")
plt.ylabel("Number of Nights")
plt.legend()

f2 = plt.figure(2)
plt.plot(date_res, nsbnratae_gr, label="Greece")
plt.plot(date_res, nsbnratae_es, label="Spain")
plt.title("Nights spent by non-residents at tourist accommodation establishments")
plt.xlabel("Dates (Year - Month)")
plt.ylabel("Number of Nights")
plt.legend()

f3 = plt.figure(3)
plt.plot(date_res, aatae_gr, label="Greece")
plt.plot(date_res, aatae_es, label="Spain")
plt.title("Arrivals at tourist accommodation establishments")
plt.xlabel("Dates (Year - Month)")
plt.ylabel("Number of Arrivals")
plt.legend()

f4 = plt.figure(4)
plt.plot(date_res, aonratae_gr, label="Greece")
plt.plot(date_res, aonratae_es, label="Spain")
plt.title("Arrivals of non-residents at tourist accommodation establishments")
plt.xlabel("Dates (Year - Month)")
plt.ylabel("Number of Arrivals")
plt.legend()

plt.show()


