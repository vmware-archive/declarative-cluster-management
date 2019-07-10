include "globals.mzn";

%%%%%%%%%%%%%%%
%   HELPERS   %
%%%%%%%%%%%%%%%

<#--
Generic comprehension that given a string and an array of strings, it returns the index of string in that array
This is used by the convenience INDEX functions from the string-to-int mapping
-->
% returns the index of a string array
% https://stackoverflow.com/a/44846993
int: index_of(string: str, array[int] of string: strarr) =
  sum([ if str = strarr[i] then i else 0 endif | i in index_set(strarr) ]);

% returns the index of a value in an array
var int: index_of(var int: v, array[int] of int: arr) =
  sum([ if v = arr[i] then i else 0 endif | i in index_set(arr) ]);


%%%%%%% Helper methods for group bys
function var int: pairing(var int: k1, var int: k2, var int: k3, var int: k4) =  pairing(pairing(k1, k2, k3), k4);
function int: pairing(int: k1, int: k2, int: k3, int: k4) =  pairing(pairing(k1, k2, k3), k4);


function var int: pairing(var int: k1, var int: k2, var int: k3) =  pairing(pairing(k1, k2), k3);
function int: pairing(int: k1, int: k2, int: k3) = pairing(pairing(k1, k2), k3);


function var int: pairing(var int: k1, var int: k2) = ((k1 + k2) * (k1 + k2 + 1) div 2) + k2;
function int: pairing(int: k1, int: k2) = assert(k1 >= 0 /\ k2 >= 0, "pairing only works with x, y >= 0", ((k1 + k2) * (k1 + k2 + 1) div 2) + k2);


function  set of int: uniqueTupleIndices2(array[int] of  int: col1, array[int] of  int: col2) =
   uniqueTupleIndices2(col1, col2, {}, {}, 1);

function  set of int: uniqueTupleIndices2(array[int] of  int: col1, array[int] of  int: col2,  set of int: seen, set of int: indices, int: currIndex) =
   if currIndex > length(col1)
      then indices
   elseif pairing(col1[currIndex], col2[currIndex]) in seen
      then uniqueTupleIndices2(col1, col2, seen, indices, currIndex + 1)
   else
      uniqueTupleIndices2(col1, col2, (seen union {pairing(col1[currIndex], col2[currIndex])}), (indices union {currIndex}), currIndex + 1)
   endif
;

function var set of int: uniqueTupleIndices2(array[int] of var int: col1, array[int] of var int: col2) =
   uniqueTupleIndices2(col1, col2, {}, {}, 1);

function var set of int: uniqueTupleIndices2(array[int] of var int: col1, array[int] of var int: col2, var set of int: seen, set of int: indices, int: currIndex) =
   if currIndex > length(col1)
      then indices
   elseif pairing(col1[currIndex], col2[currIndex]) in seen
      then uniqueTupleIndices2(col1, col2, seen, indices, currIndex + 1)
   else
      uniqueTupleIndices2(col1, col2, (seen union {pairing(col1[currIndex], col2[currIndex])}), (indices union {currIndex}), currIndex + 1)
   endif
;

function  set of int: uniqueTupleIndices4(array[int] of  int: col1, array[int] of  int: col2, array[int] of  int: col3, array[int] of  int: col4) =
   uniqueTupleIndices4(col1, col2, col3, col4, {}, {}, 1);

function  set of int: uniqueTupleIndices4(array[int] of  int: col1, array[int] of  int: col2, array[int] of  int: col3, array[int] of  int: col4,  set of int: seen, set of int: indices, int: currIndex) =
   if currIndex > length(col1)
      then indices
   elseif pairing(col1[currIndex], col2[currIndex], col3[currIndex], col4[currIndex]) in seen
      then uniqueTupleIndices4(col1, col2, col3, col4, seen, indices, currIndex + 1)
   else
      uniqueTupleIndices4(col1, col2, col3, col4, (seen union {pairing(col1[currIndex], col2[currIndex], col3[currIndex], col4[currIndex])}), (indices union {currIndex}), currIndex + 1)
   endif
;


function var set of int: uniqueTupleIndices4(array[int] of var int: col1, array[int] of var int: col2, array[int] of var int: col3, array[int] of var int: col4) =
   uniqueTupleIndices4(col1, col2, col3, col4, {}, {}, 1);

function var set of int: uniqueTupleIndices4(array[int] of var int: col1, array[int] of var int: col2, array[int] of var int: col3, array[int] of var int: col4, var set of int: seen, set of int: indices, int: currIndex) =
   if currIndex > length(col1)
      then indices
   elseif pairing(col1[currIndex], col2[currIndex], col3[currIndex], col4[currIndex]) in seen
      then uniqueTupleIndices4(col1, col2, col3, col4, seen, indices, currIndex + 1)
   else
      uniqueTupleIndices4(col1, col2, col3, col4, (seen union {pairing(col1[currIndex], col2[currIndex], col3[currIndex], col4[currIndex])}), (indices union {currIndex}), currIndex + 1)
   endif
;

predicate member(array[int] of var opt int: x, var int: y) = exists(i in index_set(x)) ( x[i] == y );

predicate all_equal(array[int] of var opt int: x) = forall(i, j in index_set(x) where i < j) ( x[i] = x[j] );

function set of int: uniqueTupleIndices1(array[int] of int: col1) = 1..length(col1);
function var set of int: uniqueTupleIndices1(array[int] of var int: col1) = 1..length(col1);
function var set of int: uniqueTupleIndices1(array[int] of var opt int: col1) = 1..sum([1 | i in col1]);
predicate exists_custom(array[int] of var opt int: col1) = sum([1 | i in col1]) > 0;
predicate exists_custom(array[int] of var int: col1) = length(col1) > 0;
enum STRING_LITERALS;

%%%%%%%%%%%%%%%
% TABLES      %
%%%%%%%%%%%%%%%

<#list arrayDeclarations as declaration>
${declaration}

</#list>


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% NON-CONSTRAINT VIEWS      %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

<#list nonConstraintViewCode as declaration>
${declaration}

</#list>

%%%%%%%%%%%%%%%
% CONSTRAINTS %
%%%%%%%%%%%%%%%

<#list constraintViewCode as declaration>
${declaration}

</#list>

%%%%%%%%%%%%%%%
% OBJECTIVES  %
%%%%%%%%%%%%%%%

<#list objectiveFunctionsCode as declaration>
${declaration}
</#list>