# $Id: test-2-storage.gnuplot,v 1.3 2004/10/24 02:27:18 srhea Exp $
set xlabel "Time (hours)"
set ylabel "Total Storage (MB)"
set xtics 0,1
set mxtics 2
set yrange [0:11]
set mytics 2
set key 8.3,10.5
set arrow from 4.0,11 to 4.0,0 nohead lt 4
set arrow from 5.5,11 to 5.5,0 nohead lt 4
set arrow from 6.0,11 to 6.0,0 nohead lt 4
set terminal postscript eps "Times-Roman" 30
set output 'test-2-storage.eps'
set arrow 
plot 'test-2-storage.dat' us ($1/1000/3600):($2/1024/1024) \
     ti "Client 1" wi li lw 3, \
     'test-2-storage.dat' us ($1/1000/3600):($3/1024/1024) \
     ti "Client 2" wi li lw 3, \
     'test-2-storage.dat' us ($1/1000/3600):($4/1024/1024) \
     ti "Client 3" wi li lw 3
#'test-2-storage.dat' us ($1/1000/3600):($5/1024/1024) ti "4" with li lw 3, \
#'test-2-storage.dat' us ($1/1000/3600):($6/1024/1024) ti "5" with li lw 3
#pause -1
