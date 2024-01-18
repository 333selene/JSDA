setlocal enabledelayedexpansion
d:
call yd d:\intdaily\japan\jsda\add
del newdat* *.mod *.cre *.aud *.csv
call btime
call envset

copy f:\intdaily\japan\jsda\add\i
copy d:\intdaily\japan\jsda\compiled\*.csv
copy d:\intdaily\japan\jsda\jsda\all.csv
 
for /L %%A in (2,1,%yy%) do (
    if %%A LSS 10 (
        if exist 200%%A.csv (
            call csv2modb 200%%A.csv add200%%A.dic 0%%A.mod
            )
        )
    if %%A GEQ 10 (
        if exist 20%%A.csv (
            call csv2modb 20%%A.csv add20%%A.dic %%A.mod
            )
        )
    )
for /L %%A in (2,1,%yy%) do (
    if %%A LSS 10 (
        if exist add200%%Asplit_JP.dic (
            call csv2modb all.csv add200%%Asplit_JP.dic 0%%Asplit.mod
            )
        )
    if %%A GEQ 10 (
        if %%A LEQ 15 (
            if exist add20%%Asplit_JP.dic (
                call csv2modb all.csv add20%%Asplit_JP.dic %%Asplit.mod
                call dwsplit %%Asplit.mod g:\util\%%Adaily.txt
                call del %%Asplit.mod
                call ren out.mod %%Asplit.mod
                )
            )
        if %%A GTR 15 (
            if exist add20%%Asplit_JP.dic (
                call csv2modb all.csv add20%%Asplit_JP.dic %%Asplit_jp.mod
                )
            if exist add20%%Asplit.dic (
                call csv2modb all.csv add20%%Asplit.dic %%Asplit_eng.mod
                )
            if exist %%Asplit_jp.mod (
                del %%Asplit.mod
                copy %%Asplit_jp.mod+%%Asplit_eng.mod %%Asplit.mod
                )
            if not exist %%Asplit_jp.mod ( 
                del %%Asplit.mod
                ren %%Asplit_end.mod %%Asplit.mod
                )
            call dwsplit %%Asplit.mod g:\util\%%Adaily.txt
            call del %%Asplit.mod %%Asplit_jp.mod %%Asplit_eng.mod
            call ren out.mod %%Asplit.mod
            )
        )
    )
 


call copy *.mod add.mod
call mod2cre <i
pause


call shelldb usd
call setl
call colorful red "CONFIRM TARGET DB SETL = NEWDAT. DOUBLE CHECK DLXFED TARGET!"
pause
call colorful red "CONFIRM TARGET DB. DONT FED A CRE FILE TO INTDAILY"
pause
call colorful red "CONFIRM TARGET DB SETL = NEWDAT. DOUBLE CHECK DLXFED TARGET!"
pause


call dlxfed add.cre
pause
call dlxfed add.mod 
pause
call dlxfed new.par
pause
call dlxfed new.lab
pause

call copy NUL newdat.kda
call copy NUL newdat.cci
call copy NUL newdat.gap
call copy NUL newdat.prs
call copy NUL newdat.xms
call copy NUL newdat.dps
