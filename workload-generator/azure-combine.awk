BEGIN {
	count = 0;
	count2 = 0;
	count3 = 0;
} 

FNR == 1 { 
	++fIndex 
} 

fIndex == 1 {
	if (!($1 in dep)) {
		dep[$1] = count; 
		count += 1;
	}
	next
}

fIndex == 2 {
	if (! ($1 in subsc)) {
		subsc[$1] = count2;
		count2 += 1; 
	}
	next 
}

fIndex == 3 {
	if (! ($1 in vms)) {
		vms[$1] = count3;
		count3 += 1;
	}
	print vms[$1],  subsc[$2], dep[$3], $4, $5, $10, $11, $6
}
