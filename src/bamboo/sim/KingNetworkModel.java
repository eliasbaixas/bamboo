/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.StreamTokenizer;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.Reader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Parses topologies from the
 * <a href="http://www.pdos.lcs.mit.edu/p2psim/kingdata/">King data set</a>
 * for use under the Bamboo simulator.
 */
public class KingNetworkModel implements NetworkModel {

    public Logger logger;
    public int [][] latencies;

    public KingNetworkModel (String filename) throws IOException {

        logger = Logger.getLogger (KingNetworkModel.class);

	FileInputStream is = new FileInputStream (filename);
	Reader reader = new BufferedReader (new InputStreamReader (is));
	StreamTokenizer tok = new StreamTokenizer (reader);

	logger.debug ("filename=" + filename);
	tok.resetSyntax ();
	tok.whitespaceChars ((int) ',', (int) ',');
	tok.whitespaceChars ((int) ' ', (int) ' ');
	tok.wordChars ((int) '_', (int) '_');
	tok.wordChars ((int) 'A', (int) 'Z');
	tok.wordChars ((int) 'a', (int) 'z');
	tok.wordChars ((int) '0', (int) '9');

	int lno = 1;
	while (lno < 5) {
            tok.nextToken ();
            if (tok.ttype == StreamTokenizer.TT_EOL)
                lno++;
	}

        int nodes = 0;
        while (true) {

            tok.nextToken ();
            if ((tok.ttype != StreamTokenizer.TT_WORD) ||
                (! tok.sval.equals ("node"))) {
                break;
            }

            tok.nextToken ();
            if (tok.ttype != StreamTokenizer.TT_WORD)
                break;

            int n = Integer.parseInt (tok.sval);
            if (n != nodes+1) {
                logger.fatal ("skipped a node number: " + n + ", line " + lno);
                System.exit (1);
            }

            tok.nextToken ();
            if (tok.ttype != StreamTokenizer.TT_EOL) {
                logger.fatal ("expected a new line, line " + lno);
                System.exit (1);
            }

            ++lno;
            ++nodes;
        }

        logger.info ("topology has " + nodes + " nodes");

        latencies = new int [nodes+1][];
        for (int i = 1; i < nodes+1; ++i)
            latencies [i] = new int [nodes+1];

        for (int i = 1; i < nodes+1; ++i) {
            for (int j = i + 1; j < nodes+1; ++j) {

                if (tok.ttype != StreamTokenizer.TT_WORD) {
                    logger.fatal ("expected a number, line " + lno);
                    System.exit (1);
                }

                if (Integer.parseInt (tok.sval) != i) {
                    logger.fatal ("expected first number to be " + i
                            + ", line " + lno);
                    System.exit (1);
                }

                /*
                tok.nextToken ();
                if (tok.ttype != StreamTokenizer.TT_WORD) {
                    logger.fatal ("expected a comma " + lno);
                    System.exit (1);
                }
                */

                tok.nextToken ();
                if (tok.ttype != StreamTokenizer.TT_WORD) {
                    logger.fatal ("expected a number, line " + lno);
                    System.exit (1);
                }

                if (Integer.parseInt (tok.sval) != j) {
                    logger.fatal ("expected second number to be " + j
                            + ", line " + lno);
                    System.exit (1);
                }

                tok.nextToken ();
                if (tok.ttype != StreamTokenizer.TT_WORD) {
                    logger.fatal ("expected a latency, line " + lno);
                    System.exit (1);
                }

                latencies[i][j] = latencies[j][i] = Integer.parseInt (tok.sval);

                tok.nextToken ();
                if (tok.ttype != StreamTokenizer.TT_EOL) {
                    logger.fatal ("expected a new line, line " + lno);
                    System.exit (1);
                }

                ++lno;
                tok.nextToken ();
            }
        }
    }

    public NetworkModel.RouteInfo compute_route_info (int src, int dst) {
        if (src <= 0 || src >= latencies.length)
            throw new IllegalArgumentException ("src=" + src);
        if (dst <= 0 || dst >= latencies [src].length)
            throw new IllegalArgumentException ("dst=" + dst);
        return new NetworkModel.RouteInfo (latencies [src][dst]*1000, 0);
    }
}

