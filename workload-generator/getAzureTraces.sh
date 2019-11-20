wget -O v1-deployment.csv.gz http://azurepublicdataset.blob.core.windows.net/azurepublicdataset/trace_data/deployment/deployment.csv.gz
wget -O v1-subscriptions.csv.gz http://azurepublicdataset.blob.core.windows.net/azurepublicdataset/trace_data/subscriptions/subscriptions.csv.gz 
wget -O v1-vmtable.csv.gz http://azurepublicdataset.blob.core.windows.net/azurepublicdataset/trace_data/vmtable/vmtable.csv.gz 
gunzip v1-deployment.csv.gz
gunzip v1-subscriptions.csv.gz
gunzip v1-vmtable.csv.gz

wget -O v2-deployment.csv.gz http://azurepublicdatasetv2.blob.core.windows.net/azurepublicdatasetv2/trace_data/deployments/deployments.csv.gz
wget -O v2-subscriptions.csv.gz http://azurepublicdatasetv2.blob.core.windows.net/azurepublicdatasetv2/trace_data/subscriptions/subscriptions.csv.gz
wget -O v2-vmtable.csv.gz http://azurepublicdatasetv2.blob.core.windows.net/azurepublicdatasetv2/trace_data/vmtable/vmtable.csv.gz
gunzip v2-deployment.csv.gz
gunzip v2-subscriptions.csv.gz
gunzip v2-vmtable.csv.gz

for ver in v1 v2; do
	awk -F "," -f azure-combine.awk $ver-deployment.csv $ver-subscriptions.csv $ver-vmtable.csv > $ver-temp-1.txt
	sed -e "s///g" $ver-temp-1.txt > $ver-temp-2.txt
	sort -nk2,2 -nk3,3 -nk4,4 -nk5,5 $ver-temp-2.txt > $ver-temp-3.txt 
	cat $ver-temp-3.txt | awk '{if($4 != $5) a[$2," ",$3," ",$4," ",$5," ",$6," ",$7]+=1;}END{for (key in a) print key, a[key]}' > $ver-temp-4.txt
	sed -e "s///g" $ver-temp-4.txt > $ver-temp-5.txt
	sort -nk3,3 -nk1,1 -nk2,2 $ver-temp-5.txt > $ver-temp-6.txt
	sed -e "s///g" $ver-temp-6.txt > $ver-data.txt
	rm *-temp-*
done

cat v1-data.txt | awk '{if(NR>22390) print $0}' > v1-cropped.txt
cat v2-data.txt | awk '{if(NR>27326) print $0}' > v2-cropped.txt
cp {v1-data.txt,v2-data.txt,v1-cropped.txt,v2-cropped.txt} ../k8s-scheduler/src/test/resources/
