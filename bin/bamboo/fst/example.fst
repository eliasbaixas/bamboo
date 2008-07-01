# $Id: example.fst,v 1.1 2005/04/27 06:03:40 srhea Exp $
# Global parameters:
# maxTTL maxPut rateBps
      60   1000    1000
# For each client, pmult used to compute the client's put rate.  The client
# will put once every pmult*fp seconds, where fp is the period corresponding
# to the fair rate for a client doing maximum-size, maximum-TTL puts.
# pmult  size   ttl
    0.5  1000    60 
    1.0  1000    60
    2.0  1000    60
