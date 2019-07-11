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

predicate member(array[int] of var opt int: x, var int: y) = exists(i in index_set(x)) ( x[i] == y );

predicate all_equal(array[int] of var opt int: x) = forall(i, j in index_set(x) where i < j) ( x[i] = x[j] );

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