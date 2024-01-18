@echo off
setlocal enabledelayedexpansion

d:
call yd d:\intdaily\japan\jsda\jsda\
copy f:\intdaily\japan\jsda\jsda\i

call btime
call envset
del *.DES *.PAR *.UPD
call clean
call today
pause

setn intdaily
more disc.aud
pause

call dlxfed disc.lab
pause
call dlxlist < i
pause
call dlxfed %month%%day%J74.DES 
pause
call copyup intdaily
