/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import ostore.util.ByteArrayInputBuffer;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.CountBuffer;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QSString;
import ostore.util.QuickSerializable;
import ostore.util.TypeTable;

/**
 * A QuickSerializable signed object.
 *
 * @author Sean C. Rhea
 * @version $Id: Certificate.java,v 1.3 2004/04/20 17:43:49 srhea Exp $
 */
public class Certificate implements QuickSerializable {

    public static void public_key_to_buffer (OutputBuffer buffer,
                                             PublicKey public_key) throws
        NoSuchAlgorithmException, InvalidKeySpecException {
        String key_alg = public_key.getAlgorithm();
        KeyFactory kf = KeyFactory.getInstance(key_alg);
        X509EncodedKeySpec ks = (X509EncodedKeySpec)
            kf.getKeySpec (public_key, X509EncodedKeySpec.class);
        byte[] key_bytes = ks.getEncoded();
        buffer.add (key_alg);
        buffer.add (key_bytes.length);
        buffer.add (key_bytes, 0, key_bytes.length);
    }

    public static PublicKey buffer_to_public_key(InputBuffer ib) throws
        NoSuchAlgorithmException, InvalidKeySpecException {
        String key_alg = ib.nextString();
        int key_len = ib.nextInt();
        byte[] key_bytes = new byte[key_len];
        ib.nextBytes(key_bytes, 0, key_len);
        X509EncodedKeySpec key_spec = new X509EncodedKeySpec(key_bytes);
        KeyFactory kf = KeyFactory.getInstance(key_alg);
        return kf.generatePublic(key_spec);
    }

    public static class InvalidException
        extends Exception {
        public InvalidException(String s) {
            super(s);
        }
    }

    protected String sig_alg;
    protected QuickSerializable signed_value;
    protected PublicKey public_key;
    protected byte [] cert_bytes;
    protected byte [] sig_bytes;
    protected boolean checked;

    public Certificate (String sa, QuickSerializable s, KeyPair p)
    throws InvalidException {

        sig_alg = sa; signed_value = s;
        public_key = p.getPublic ();

        OutputBuffer ob = null;
        for (int i = 0; i < 2; ++i) {
            if (i == 0)
                ob = new CountBuffer ();
            else {
                cert_bytes = new byte [((CountBuffer) ob).size ()];
                ob = new ByteArrayOutputBuffer (cert_bytes);
            }
            ob.add(sig_alg);
            try {
                public_key_to_buffer(ob, public_key);
            }
            catch (NoSuchAlgorithmException ex1) {
                throw new InvalidException("no such algorithm: "
                                           + public_key.getAlgorithm ());
            }
            catch (InvalidKeySpecException ex) {
                assert false;
            }
            ob.add(signed_value);
        }

        Signature sig = null;
        try {
            sig = Signature.getInstance(sig_alg);
        }
        catch (NoSuchAlgorithmException ex1) {
            throw new InvalidException("no such algorithm: " + sig_alg);
        }
        try {
            sig.initSign (p.getPrivate ());
        }
        catch (InvalidKeyException ex3) {
            throw new InvalidException ("invalid key");
        }
        try {
            sig.update (cert_bytes);
            sig_bytes = sig.sign();
        }
        catch (SignatureException ex2) {
            throw new InvalidException ("signature exception");
        }
    }

    public Certificate (InputBuffer buffer) {
        int cert_len = buffer.nextInt ();
        cert_bytes = new byte [cert_len];
        buffer.nextBytes (cert_bytes, 0, cert_len);
        int sig_len = buffer.nextInt ();
        sig_bytes = new byte [sig_len];
        buffer.nextBytes (sig_bytes, 0, sig_len);
    }

    public void serialize (OutputBuffer buffer) {
        buffer.add (cert_bytes.length);
        buffer.add (cert_bytes);
        buffer.add (sig_bytes.length);
        buffer.add (sig_bytes);
    }

    public String sig_alg () throws InvalidException {
        if (! checked)
            check ();
        return sig_alg;
    }

    public PublicKey public_key () throws InvalidException {
        if (! checked)
            check ();
        return public_key;
    }

    public QuickSerializable signed_value () throws InvalidException {
        if (! checked)
            check ();
        return signed_value;
    }

    protected void check () throws InvalidException {
        ByteArrayInputBuffer ib = new ByteArrayInputBuffer(cert_bytes);
        sig_alg = ib.nextString();

        try {
            public_key = buffer_to_public_key (ib);
        }
        catch (NoSuchAlgorithmException e) {
            throw new InvalidException("no such algorithm: " + e);
        }
        catch (InvalidKeySpecException e) {
            throw new InvalidException("bad key encoding: " + e);
        }

        Signature sig = null;
        try {
            sig = Signature.getInstance(sig_alg);
        }
        catch (NoSuchAlgorithmException e) {
            throw new InvalidException("no such algorithm: " + e);
        }

        try {
            sig.initVerify (public_key);
        }
        catch (InvalidKeyException e) {
            throw new InvalidException ("invalid key: " + e);
        }

        try {
            sig.update (cert_bytes);
            if (! sig.verify (sig_bytes))
                throw new InvalidException ("signature didn't verify");
        }
        catch (SignatureException e) {
            throw new InvalidException ("signature exception: " + e);
        }

        try {
            signed_value = ib.nextObject();
        }
        catch (QSException e) {
            throw new InvalidException ("value decoding error: " + e);
        }

        checked = true;
    }

    static public void main (String [] args) throws Exception {
        String key_alg = "RSA";
        String sig_alg = "SHA1withRSA";
        QSString cert_value = new QSString ("Hello, world!");
        TypeTable.register_type (QSString.class);
        TypeTable.register_type (Certificate.class);
        KeyPairGenerator kgen = KeyPairGenerator.getInstance (key_alg);
        kgen.initialize(1024);
        KeyPair kp = kgen.generateKeyPair ();
        Certificate cert = new Certificate (sig_alg, cert_value, kp);
        CountBuffer cb = new CountBuffer ();
        cb.add (cert_value);
        System.err.println("payload size is " + cb.size () + " bytes");
        cb = new CountBuffer ();
        cert.serialize(cb);
        System.err.println("certificate size is " + cb.size () + " bytes");
        byte [] bytes = new byte [cb.size ()];
        ByteArrayOutputBuffer ob = new ByteArrayOutputBuffer (bytes);
        cert.serialize (ob);
        ByteArrayInputBuffer ib = new ByteArrayInputBuffer (bytes);
        cert = new Certificate (ib);
        cert.signed_value ();
        bytes [bytes.length - 1] ^= 1; // change one bit of the signature
        ib = new ByteArrayInputBuffer (bytes);
        cert = new Certificate (ib);
        boolean failed = false;
        try {
            cert.signed_value();
        }
        catch (InvalidException e) {
            if (! e.getMessage ().equals ("signature didn't verify"))
                throw e;
            failed = true;
        }
        if (failed)
            System.err.println ("test passed");
        else
            System.err.println("bad signature verified!");
    }
}
