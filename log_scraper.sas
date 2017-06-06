/* This code scrapes INPUTs and OUTPUTs from the log and the codes*/
options mprint mlogic symbolgen spool=yes source source2 armsubsys=(arm_none) notes noerrorabend;
options nomprint nomlogic nosymbolgen spool=no nosource nosource2 armsubsys=(arm_none) nonotes noerrorabend;

libname uttam "/uttam/kumar";

%let lib_to_search=/uttam/kumar/logs;

%macro read_logs();

options mprint mlogic symbolgen spool=yes source source2 armsubsys=(arm_none) notes noerrorabend;

filename x pipe "find &lib_to_search. -maxdepth 1 -type f \( -iname '*.log' \) -exec ls -lrt --full-time {} +;

data work.logfile_info(keep=filename size datetime pathname);
infile x truncover;
length path $1000. access $10. file_or_dir 8. id $30. group $30. size $16. sp_pr $10.;
format date date9. time time. datetime datetime20.;
input All $2000.;
path=substr(All,find(All,"/"));
access=scan(All,1," ");
file_or_dir=scan(All,2," ");
id=scan(All,3," ");
group=scan(All,4," ");
size=scan(All,5," ");
date=input(scan(All,6," "),yymmdd10.);
time=input(substr(scan(All,7," "),1,find(scan(All,7," "),".")-1),time10.);
datetime=input(put(date,date9.)||":"||strip(put(time,time10.)),datetime20.);
sp_per=scan(All,8," ");
filename=scan(path,-1,"/");
pathname=trim(left(substr(path,1,find(path,"/",-length(path)))));
run;


/*Extracting flow and job name from the log file*/

data logfile_info(keep=filename size datetime pathname flow_job);
length year year_prev $4. flow_job $1000.;
set logfile_info;
today=today();
year=year(today);
year_pos=find(filename,year)-1;
flow_job=substr(filename,1,compress(find(filename,".log")-1));
if year_pos > 0 then flow_job=substr(filename,1,year_Pos);
else do;
year_prev=year(intnx('year',today,-1));
year_pos=find(filename,year_prev)-1;
if year_pos > 0 then flow_job=substr(filename,1,year_pos);
end;
run;


proc sort data=logfile_info;
by flow_job descending datetime;
run;

/*Picking out the latest instance of the log file*/

data logfile_info_latest(drop=pathname flag flag_2);
set logfile_info;
retain flag;
by flow_job descending datetime;
flag_2="N";
if size > 1073741824 then 
 size_in_GB_MB_KB=compress(put(size/(1024*1024*1024),8,2||"GB");
else if size > 1048576 then 
 size_in_GB_MB_KB=compress(put(size/(1024*1024),8,2||"MB");
else if size > 1024 then 
 size_in_GB_MB_KB=compress(put(size/(1024),8,2||"KB");
else if size < 1024 then 
 size_in_GB_MB_KB=compress(size||"B");
if first.flow_job=1 and last.flow_job then flag_2="Y";
if first.flow_job then flag=1;
else flag=flag+1;
if flag=2 then flag_2="Y";
if flag_2="Y";
/**209715200 bytes is 200MB*/
if size < 209715200;
run;

proc sort data=logfile_info_latest;
by flow_job;
run;


/**********************************************/
/**********************************************/

data flow_info;
length flow_job $500.;
set uttam.flow_info(keep=jobname flowname);
flow_job=strip(strip(flowname)||"_"||strip(jobname)||"_");
run;

proc sort data=flow_info;
by flow_job;
run;

data flow_job_info_latest;
merge logfile_info_latest(in=a) flow_info(in=b);
by flow_job;
run;
if a;
run;


data all_libs;
length libname $15.;
set sashelp.VLIBNAM(keep=libname rename=(ibname=libname_old));
libname='"'||strip(libname_old)||'"';
run;

proc sql noprint;
select count(distinc libname) into:num_libs separated by "" from all_libs;
select distinct libname into:All_libs separated by " " from all_libs;
select distinct libname into:all_libs_comma separated by "," from all_libs;
quit;

%mend;

%read_logs;


%macro extract_log(logname=,flowname=,jobname=);

data input_all_1;
infile "&lib_to_search./&logname." truncover expandtabs;
retain flag_dataP flag_comments;
input All $5000. data_or_proc $10. step $10.;
all=strip(compbl(_infile_));
all=compress(all,"-");
if find(All,"The SAS System",'i') then delete;
if length(All)=1 then delete;
if substr(All,1,6)="GLOBAL" then delete;
if substr(All,1,9)="AUTOMATIC" then delete;
if find(All,'let _INPUT_') then delete;
if find(All,'let _OUTPUT_') then delete;
if find(All,'Version: SAS Data Integration Studio') then delete;
/* Taking Care of the comments*/
if find(All,"/*") and find(All,"*/") then do; All=prxchange("s/(\/\*) *\w.*(\*\/)//",1,All);end;
if find(All,"/*") then do;flag_comments=1;end;
if find(All,"*/") then do;flag_comments=0;flag_comments_end=1;All=strip(strip(All)||";");end;
if flag_comments=1 or flag_comments_end=1 then step="COMMENTS";
if step="COMMENTS" then delete;

if find(All,'PUT ','i') then do;
if find(All,'PUT "','i') then delete;
if prxmatch("/^\( *\d+\) \%*put /i",All) then delete;
if prxmatch("/^\( *\d+\) *PUT */i",All) then delete;
if prxmatch("/MPRINT\(\w+\): *put/i",All) then delete;
end;

if notdigit(All) > length(All) then delete;
if find(All,' data ','i') and find(All,"NOTE:")=0 then do;data_or_proc="DATA";flag_dataP=1;end;
if find(All,' proc ','i') then do;data_or_proc="PROC";flag_dataP=1;end;

if length(All) > 5 then do;
if substr(strip(All),1,4)="NOTE" then do;step="NOTE";flag1=1;All=strip(strip(All)||";");end;
if flag1=1 then do;
if find(All,"observation",'i') > 0 or find(All,"created, with",'i') > 0 find(All,".VIEW used") then flag_note=1;
end;

if find(All,'let SYSLAST = ') then do;step="SYSLAST";flag_sysIO=1;end;
if find(All,'let _INPUT') then do;step="_INPUT";flag_sysIO=1;
if prxmatch("/\d+ \%*let +w*INPUT\d*_\w*/i",All) then delete;
end;
if find(All,'let _OUTPUT') then do;step="_OUTPUT";flag_sysIO=1;
if prxmatch("/\d+ \%*let +w*OUTPUT\d*_\w*/i",All) then delete;
end;

if find(All,'SYMBOLGEN:') then do;step="SYMBOLGEN";flag_symbolgen=1;All=strip(All)||";)"end;

if find(All,' run;','i') or find(All,' quit;','i') or find(All,'|','i') or find(All,' run ;','i') or find(All,' quit ;','i') or find(All,"NOTE:",'i') then do;flag_dataP=0;end;
if find(All,' run;','i') or find(All,' quit;','i') or find(All,' run ;','i') or find(All,' quit ;','i') then do;flag_runq=1;end;
if flag_dataP=1 or flag_note=1 or flag_sysIO=1 or flag_runq=1 or flag_symbolgen=1 or flag_comments=1;
run;

data _NULL_;
ss=pathname('work');
first_path=cat("/",compress(scan(ss,1,"/")),"/",compress(scan(ss,2,"/")),"/",compress(scan(ss,3,"/")));
call symputx('work_path',ss);
call symputx('first_path',first_path);
run;

x "chmod 777 &first_path.";
x "chmod 777 &work_path.";


data _NULL_;
file "&work_path./&logname";
set input_all_1;
put All;
run;


data input_all_clean_2;
length All $32767. data_or_proc $10. step $10. section $10.;
retain data_or_proc;
infile "&work_path./&logname" recfm=n dsd dlm=";" lrecl=32767;
input All $;
All=strip(left(All));
All=TRANWRD(All,'0D'x,'');
Alll=TRANWRD(All,'0A'x,'');
All=strip(left(All));
All=compbl(All);
/*if find statements starting with digits then its part of the code*/
if prxmatch("/^d+ */",All) then do;
/*remove the digits before the code part*/
All=prxchange("s/^d+ *//",1All);section="CODE";
end;
if prxmatch("/MPRINT\(\w+\):+ */",All) then do;
All=prxchange("s/MPRINT\(\w+\):+ *//",1,All);section="MPRINT";
end;

if upcase(substr(All,1,4))="DATA" then do;data_or_proc="DATA";flag_dataP=1;end;
if upcase(substr(All,1,4))="PROC" then do;data_or_proc="PROC";flag_dataP=1;end;

if length(All) > 5 then do;
if substr(strip(All,1,4)="NOTE" then do;step="NOTE";flag1=1;data_orProc="";end;
end;


if length(All) > 9 then do;
if substr(strip(All,1,9)="SYMBOLGEN" then do;step="SYMBOLGEN";flag_symbolgen=1;end;
end;

if flag1=1 then do;
if find(All,"observation",'i') > 0 or find(All,"created, with",'i') > 0 find(All,".VIEW used") then flag_note=1;
end;

if find(All,'let SYSLAST = ') then do;step="SYSLAST";flag_sysIO=1;end;
if find(All,'let _INPUT') then do;step="_INPUT";flag_sysIO=1;
end;
if find(All,'let _OUTPUT') then do;step="_OUTPUT";flag_sysIO=1;end;

if upcase(substr(All,1,3))="RUN" or upcase(substr(All,1,4))="QUIT" or find(All,'|','i') then do; flag_dataP=0;data_or_proc="";end;
if upcase(substr(All,1,3))="RUN" or upcase(substr(All,1,4))="QUIT" then do; flag_runq=0;end;
run;



data input_all_extract_3;
length dataset $500.;
set input_all_clean2;
ExpressionID = prxparse('/\w+\.[\w\&\.]+/');
start=1;
stop=length(All);
call prxnext(ExpressionID,start,stop,All,postion,length);
do while(position > 0);
dataset=strip(substr(All,postion,length));
call prxnext(ExpressionID,start,stop,All,position,length);
output;
end;
run;





























































