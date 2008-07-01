# $Id: test-1-commitment.gnuplot,v 1.1 2004/09/16 20:46:31 srhea Exp $
set xlabel "Time (min)"
set ylabel "Acheived rate (puts/s)"
set xtics 0,60
set mxtics 4
set key 240,0.27
set terminal postscript eps color solid "Times-Roman" 30
set output 'test-1-commitment.eps'
plot 'test-1-commitment.dat' using ($1/1000/60):($2/60/60/1024) \
    ti "10/s" with li lt 1, \
     'test-1-commitment.dat' using ($1/1000/60):($3/60/60/1024) \
     ti "1/s" with li lt 2, \
     'test-1-commitment.dat' using ($1/1000/60):($4/60/60/1024) \
     ti "1/10s" with li lt 3, \
     'test-1-commitment.dat' using ($1/1000/60):($5/60/60/1024) \
     noti with li lt 3, \
     'test-1-commitment.dat' using ($1/1000/60):($6/60/60/1024) \
     ti "1/20s" with li lt 4, \
     'test-1-commitment.dat' using ($1/1000/60):($7/60/60/1024) \
     noti with li lt 4, \
     'test-1-commitment.dat' using ($1/1000/60):($8/60/60/1024) \
     ti "1/40s" with li lt 5, \
     'test-1-commitment.dat' using ($1/1000/60):($9/60/60/1024) \
     noti with li lt 5, \
     'test-1-commitment.dat' using ($1/1000/60):($10/60/60/1024) \
     ti "1/80s" with li lt 7, \
     'test-1-commitment.dat' using ($1/1000/60):($11/60/60/1024) \
     noti with li lt 7
#pause -1
