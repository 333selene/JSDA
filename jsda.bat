call clean
call setn intdaily

call yd d:\intdaily\japan\jsda 
md compiled 
md downloads 
md files 
md add 
md jsda 
del *.mod 

del jsda\** >nul
del add\** >nul
del compiled\** >nul
del files\** >nul
del downloads\** >nul


call btime >nul
call envset >nul

call python jsda.py %*
pause

copy *.mod jsda.mod

call dlxprep jsda.mod 
