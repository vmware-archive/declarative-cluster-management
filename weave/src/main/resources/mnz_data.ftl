%%%%%%%%%%%%%%%
% TABLES      %
%%%%%%%%%%%%%%%
STRING_LITERALS = {${string_literals?join(", ")}};

<#list input_parameters as input>
${input}
</#list>