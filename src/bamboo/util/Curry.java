/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;

/**
 * Parameterized function object types and currying functions, modeled on
 * libasync's <code>wrap</code> function.  
 * 
 * <p> There are two kinds of function objects defined in this file.  "Thunks"
 * are those that return <code>void</code>, and "Functions" are those that
 * return a value.  This distinction is due to the fact that you can't declare
 * a type of <code>void</code> in a parameterized Java object.  Also, the name
 * of each kind of function object includes its argument count, since you
 * can't have an unbounded number of parameters to a type.  So
 * <code>Thunk1&lt;Integer&gt;</code> takes an <code>Integer</code> argument
 * and has return type <code>void</code>, whereas
 * <code>Function2&lt;Double,Integer,Boolean&gt;</code> takes two arguments,
 * one <code>Integer</code> and one <code>Boolean</code> and returns a
 * <code>Double</code>.
 *
 * <p> There is no Thunk0; instead, we use java.lang.Runnable so that we can
 * easily use these function objects with other Java libraries--such as
 * Swing--that use Runnable for function objects.
 *
 * <i>Note: this class is automatically generated
 * by <code>Curry.pl</code>; do not edit the <code>.java</code> file.</i>
 */
public class Curry {

    public interface Thunk1<A1> {
        void run(A1 a1);
    }

    public interface Thunk2<A1, A2> {
        void run(A1 a1, A2 a2);
    }

    public interface Thunk3<A1, A2, A3> {
        void run(A1 a1, A2 a2, A3 a3);
    }

    public interface Thunk4<A1, A2, A3, A4> {
        void run(A1 a1, A2 a2, A3 a3, A4 a4);
    }

    public interface Thunk5<A1, A2, A3, A4, A5> {
        void run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5);
    }

    public interface Thunk6<A1, A2, A3, A4, A5, A6> {
        void run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6);
    }

    public interface Thunk7<A1, A2, A3, A4, A5, A6, A7> {
        void run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7);
    }

    public interface Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> {
        void run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8);
    }

    public interface Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> {
        void run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9);
    }

    public interface Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> {
        void run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10);
    }

    public interface Function0<R> {
        R run();
    }

    public interface Function1<R, A1> {
        R run(A1 a1);
    }

    public interface Function2<R, A1, A2> {
        R run(A1 a1, A2 a2);
    }

    public interface Function3<R, A1, A2, A3> {
        R run(A1 a1, A2 a2, A3 a3);
    }

    public interface Function4<R, A1, A2, A3, A4> {
        R run(A1 a1, A2 a2, A3 a3, A4 a4);
    }

    public interface Function5<R, A1, A2, A3, A4, A5> {
        R run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5);
    }

    public interface Function6<R, A1, A2, A3, A4, A5, A6> {
        R run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6);
    }

    public interface Function7<R, A1, A2, A3, A4, A5, A6, A7> {
        R run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7);
    }

    public interface Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> {
        R run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8);
    }

    public interface Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> {
        R run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9);
    }

    public interface Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> {
        R run(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10);
    }

    public static <A1> Runnable curry (
            final Thunk1<A1> f,
            final A1 a1) {
        return new Runnable () {
            public void run() {
                f.run(a1);
            }
        };
    }

    public static <A1, A2> Runnable curry (
            final Thunk2<A1, A2> f,
            final A1 a1,
            final A2 a2) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2);
            }
        };
    }

    public static <A1, A2> Thunk1<A2> curry (
            final Thunk2<A1, A2> f,
            final A1 a1) {
        return new Thunk1<A2> () {
            public void run(A2 a2) {
                f.run(a1, a2);
            }
        };
    }

    public static <A1, A2, A3> Runnable curry (
            final Thunk3<A1, A2, A3> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3);
            }
        };
    }

    public static <A1, A2, A3> Thunk1<A3> curry (
            final Thunk3<A1, A2, A3> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk1<A3> () {
            public void run(A3 a3) {
                f.run(a1, a2, a3);
            }
        };
    }

    public static <A1, A2, A3> Thunk2<A2, A3> curry (
            final Thunk3<A1, A2, A3> f,
            final A1 a1) {
        return new Thunk2<A2, A3> () {
            public void run(A2 a2, A3 a3) {
                f.run(a1, a2, a3);
            }
        };
    }

    public static <A1, A2, A3, A4> Runnable curry (
            final Thunk4<A1, A2, A3, A4> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <A1, A2, A3, A4> Thunk1<A4> curry (
            final Thunk4<A1, A2, A3, A4> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Thunk1<A4> () {
            public void run(A4 a4) {
                f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <A1, A2, A3, A4> Thunk2<A3, A4> curry (
            final Thunk4<A1, A2, A3, A4> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk2<A3, A4> () {
            public void run(A3 a3, A4 a4) {
                f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <A1, A2, A3, A4> Thunk3<A2, A3, A4> curry (
            final Thunk4<A1, A2, A3, A4> f,
            final A1 a1) {
        return new Thunk3<A2, A3, A4> () {
            public void run(A2 a2, A3 a3, A4 a4) {
                f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <A1, A2, A3, A4, A5> Runnable curry (
            final Thunk5<A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <A1, A2, A3, A4, A5> Thunk1<A5> curry (
            final Thunk5<A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Thunk1<A5> () {
            public void run(A5 a5) {
                f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <A1, A2, A3, A4, A5> Thunk2<A4, A5> curry (
            final Thunk5<A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Thunk2<A4, A5> () {
            public void run(A4 a4, A5 a5) {
                f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <A1, A2, A3, A4, A5> Thunk3<A3, A4, A5> curry (
            final Thunk5<A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk3<A3, A4, A5> () {
            public void run(A3 a3, A4 a4, A5 a5) {
                f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <A1, A2, A3, A4, A5> Thunk4<A2, A3, A4, A5> curry (
            final Thunk5<A1, A2, A3, A4, A5> f,
            final A1 a1) {
        return new Thunk4<A2, A3, A4, A5> () {
            public void run(A2 a2, A3 a3, A4 a4, A5 a5) {
                f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6> Runnable curry (
            final Thunk6<A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6> Thunk1<A6> curry (
            final Thunk6<A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Thunk1<A6> () {
            public void run(A6 a6) {
                f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6> Thunk2<A5, A6> curry (
            final Thunk6<A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Thunk2<A5, A6> () {
            public void run(A5 a5, A6 a6) {
                f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6> Thunk3<A4, A5, A6> curry (
            final Thunk6<A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Thunk3<A4, A5, A6> () {
            public void run(A4 a4, A5 a5, A6 a6) {
                f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6> Thunk4<A3, A4, A5, A6> curry (
            final Thunk6<A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk4<A3, A4, A5, A6> () {
            public void run(A3 a3, A4 a4, A5 a5, A6 a6) {
                f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6> Thunk5<A2, A3, A4, A5, A6> curry (
            final Thunk6<A1, A2, A3, A4, A5, A6> f,
            final A1 a1) {
        return new Thunk5<A2, A3, A4, A5, A6> () {
            public void run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6) {
                f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7> Runnable curry (
            final Thunk7<A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7> Thunk1<A7> curry (
            final Thunk7<A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Thunk1<A7> () {
            public void run(A7 a7) {
                f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7> Thunk2<A6, A7> curry (
            final Thunk7<A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Thunk2<A6, A7> () {
            public void run(A6 a6, A7 a7) {
                f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7> Thunk3<A5, A6, A7> curry (
            final Thunk7<A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Thunk3<A5, A6, A7> () {
            public void run(A5 a5, A6 a6, A7 a7) {
                f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7> Thunk4<A4, A5, A6, A7> curry (
            final Thunk7<A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Thunk4<A4, A5, A6, A7> () {
            public void run(A4 a4, A5 a5, A6 a6, A7 a7) {
                f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7> Thunk5<A3, A4, A5, A6, A7> curry (
            final Thunk7<A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk5<A3, A4, A5, A6, A7> () {
            public void run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7) {
                f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7> Thunk6<A2, A3, A4, A5, A6, A7> curry (
            final Thunk7<A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1) {
        return new Thunk6<A2, A3, A4, A5, A6, A7> () {
            public void run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7) {
                f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Runnable curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Thunk1<A8> curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Thunk1<A8> () {
            public void run(A8 a8) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Thunk2<A7, A8> curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Thunk2<A7, A8> () {
            public void run(A7 a7, A8 a8) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Thunk3<A6, A7, A8> curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Thunk3<A6, A7, A8> () {
            public void run(A6 a6, A7 a7, A8 a8) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Thunk4<A5, A6, A7, A8> curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Thunk4<A5, A6, A7, A8> () {
            public void run(A5 a5, A6 a6, A7 a7, A8 a8) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Thunk5<A4, A5, A6, A7, A8> curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Thunk5<A4, A5, A6, A7, A8> () {
            public void run(A4 a4, A5 a5, A6 a6, A7 a7, A8 a8) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Thunk6<A3, A4, A5, A6, A7, A8> curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk6<A3, A4, A5, A6, A7, A8> () {
            public void run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8> Thunk7<A2, A3, A4, A5, A6, A7, A8> curry (
            final Thunk8<A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1) {
        return new Thunk7<A2, A3, A4, A5, A6, A7, A8> () {
            public void run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Runnable curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8,
            final A9 a9) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk1<A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8) {
        return new Thunk1<A9> () {
            public void run(A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk2<A8, A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Thunk2<A8, A9> () {
            public void run(A8 a8, A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk3<A7, A8, A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Thunk3<A7, A8, A9> () {
            public void run(A7 a7, A8 a8, A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk4<A6, A7, A8, A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Thunk4<A6, A7, A8, A9> () {
            public void run(A6 a6, A7 a7, A8 a8, A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk5<A5, A6, A7, A8, A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Thunk5<A5, A6, A7, A8, A9> () {
            public void run(A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk6<A4, A5, A6, A7, A8, A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Thunk6<A4, A5, A6, A7, A8, A9> () {
            public void run(A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk7<A3, A4, A5, A6, A7, A8, A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk7<A3, A4, A5, A6, A7, A8, A9> () {
            public void run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9> Thunk8<A2, A3, A4, A5, A6, A7, A8, A9> curry (
            final Thunk9<A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1) {
        return new Thunk8<A2, A3, A4, A5, A6, A7, A8, A9> () {
            public void run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Runnable curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8,
            final A9 a9,
            final A10 a10) {
        return new Runnable () {
            public void run() {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk1<A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8,
            final A9 a9) {
        return new Thunk1<A10> () {
            public void run(A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk2<A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8) {
        return new Thunk2<A9, A10> () {
            public void run(A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk3<A8, A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Thunk3<A8, A9, A10> () {
            public void run(A8 a8, A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk4<A7, A8, A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Thunk4<A7, A8, A9, A10> () {
            public void run(A7 a7, A8 a8, A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk5<A6, A7, A8, A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Thunk5<A6, A7, A8, A9, A10> () {
            public void run(A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk6<A5, A6, A7, A8, A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Thunk6<A5, A6, A7, A8, A9, A10> () {
            public void run(A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk7<A4, A5, A6, A7, A8, A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Thunk7<A4, A5, A6, A7, A8, A9, A10> () {
            public void run(A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk8<A3, A4, A5, A6, A7, A8, A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2) {
        return new Thunk8<A3, A4, A5, A6, A7, A8, A9, A10> () {
            public void run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Thunk9<A2, A3, A4, A5, A6, A7, A8, A9, A10> curry (
            final Thunk10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1) {
        return new Thunk9<A2, A3, A4, A5, A6, A7, A8, A9, A10> () {
            public void run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1> Function0<R> curry (
            final Function1<R, A1> f,
            final A1 a1) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1);
            }
        };
    }

    public static <R, A1, A2> Function0<R> curry (
            final Function2<R, A1, A2> f,
            final A1 a1,
            final A2 a2) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2);
            }
        };
    }

    public static <R, A1, A2> Function1<R, A2> curry (
            final Function2<R, A1, A2> f,
            final A1 a1) {
        return new Function1<R, A2> () {
            public R run(A2 a2) {
                return f.run(a1, a2);
            }
        };
    }

    public static <R, A1, A2, A3> Function0<R> curry (
            final Function3<R, A1, A2, A3> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3);
            }
        };
    }

    public static <R, A1, A2, A3> Function1<R, A3> curry (
            final Function3<R, A1, A2, A3> f,
            final A1 a1,
            final A2 a2) {
        return new Function1<R, A3> () {
            public R run(A3 a3) {
                return f.run(a1, a2, a3);
            }
        };
    }

    public static <R, A1, A2, A3> Function2<R, A2, A3> curry (
            final Function3<R, A1, A2, A3> f,
            final A1 a1) {
        return new Function2<R, A2, A3> () {
            public R run(A2 a2, A3 a3) {
                return f.run(a1, a2, a3);
            }
        };
    }

    public static <R, A1, A2, A3, A4> Function0<R> curry (
            final Function4<R, A1, A2, A3, A4> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <R, A1, A2, A3, A4> Function1<R, A4> curry (
            final Function4<R, A1, A2, A3, A4> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function1<R, A4> () {
            public R run(A4 a4) {
                return f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <R, A1, A2, A3, A4> Function2<R, A3, A4> curry (
            final Function4<R, A1, A2, A3, A4> f,
            final A1 a1,
            final A2 a2) {
        return new Function2<R, A3, A4> () {
            public R run(A3 a3, A4 a4) {
                return f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <R, A1, A2, A3, A4> Function3<R, A2, A3, A4> curry (
            final Function4<R, A1, A2, A3, A4> f,
            final A1 a1) {
        return new Function3<R, A2, A3, A4> () {
            public R run(A2 a2, A3 a3, A4 a4) {
                return f.run(a1, a2, a3, a4);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5> Function0<R> curry (
            final Function5<R, A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5> Function1<R, A5> curry (
            final Function5<R, A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Function1<R, A5> () {
            public R run(A5 a5) {
                return f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5> Function2<R, A4, A5> curry (
            final Function5<R, A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function2<R, A4, A5> () {
            public R run(A4 a4, A5 a5) {
                return f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5> Function3<R, A3, A4, A5> curry (
            final Function5<R, A1, A2, A3, A4, A5> f,
            final A1 a1,
            final A2 a2) {
        return new Function3<R, A3, A4, A5> () {
            public R run(A3 a3, A4 a4, A5 a5) {
                return f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5> Function4<R, A2, A3, A4, A5> curry (
            final Function5<R, A1, A2, A3, A4, A5> f,
            final A1 a1) {
        return new Function4<R, A2, A3, A4, A5> () {
            public R run(A2 a2, A3 a3, A4 a4, A5 a5) {
                return f.run(a1, a2, a3, a4, a5);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6> Function0<R> curry (
            final Function6<R, A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6> Function1<R, A6> curry (
            final Function6<R, A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Function1<R, A6> () {
            public R run(A6 a6) {
                return f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6> Function2<R, A5, A6> curry (
            final Function6<R, A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Function2<R, A5, A6> () {
            public R run(A5 a5, A6 a6) {
                return f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6> Function3<R, A4, A5, A6> curry (
            final Function6<R, A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function3<R, A4, A5, A6> () {
            public R run(A4 a4, A5 a5, A6 a6) {
                return f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6> Function4<R, A3, A4, A5, A6> curry (
            final Function6<R, A1, A2, A3, A4, A5, A6> f,
            final A1 a1,
            final A2 a2) {
        return new Function4<R, A3, A4, A5, A6> () {
            public R run(A3 a3, A4 a4, A5 a5, A6 a6) {
                return f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6> Function5<R, A2, A3, A4, A5, A6> curry (
            final Function6<R, A1, A2, A3, A4, A5, A6> f,
            final A1 a1) {
        return new Function5<R, A2, A3, A4, A5, A6> () {
            public R run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6) {
                return f.run(a1, a2, a3, a4, a5, a6);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7> Function0<R> curry (
            final Function7<R, A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7> Function1<R, A7> curry (
            final Function7<R, A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Function1<R, A7> () {
            public R run(A7 a7) {
                return f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7> Function2<R, A6, A7> curry (
            final Function7<R, A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Function2<R, A6, A7> () {
            public R run(A6 a6, A7 a7) {
                return f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7> Function3<R, A5, A6, A7> curry (
            final Function7<R, A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Function3<R, A5, A6, A7> () {
            public R run(A5 a5, A6 a6, A7 a7) {
                return f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7> Function4<R, A4, A5, A6, A7> curry (
            final Function7<R, A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function4<R, A4, A5, A6, A7> () {
            public R run(A4 a4, A5 a5, A6 a6, A7 a7) {
                return f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7> Function5<R, A3, A4, A5, A6, A7> curry (
            final Function7<R, A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1,
            final A2 a2) {
        return new Function5<R, A3, A4, A5, A6, A7> () {
            public R run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7) {
                return f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7> Function6<R, A2, A3, A4, A5, A6, A7> curry (
            final Function7<R, A1, A2, A3, A4, A5, A6, A7> f,
            final A1 a1) {
        return new Function6<R, A2, A3, A4, A5, A6, A7> () {
            public R run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7) {
                return f.run(a1, a2, a3, a4, a5, a6, a7);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function0<R> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function1<R, A8> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Function1<R, A8> () {
            public R run(A8 a8) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function2<R, A7, A8> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Function2<R, A7, A8> () {
            public R run(A7 a7, A8 a8) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function3<R, A6, A7, A8> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Function3<R, A6, A7, A8> () {
            public R run(A6 a6, A7 a7, A8 a8) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function4<R, A5, A6, A7, A8> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Function4<R, A5, A6, A7, A8> () {
            public R run(A5 a5, A6 a6, A7 a7, A8 a8) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function5<R, A4, A5, A6, A7, A8> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function5<R, A4, A5, A6, A7, A8> () {
            public R run(A4 a4, A5 a5, A6 a6, A7 a7, A8 a8) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function6<R, A3, A4, A5, A6, A7, A8> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1,
            final A2 a2) {
        return new Function6<R, A3, A4, A5, A6, A7, A8> () {
            public R run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8> Function7<R, A2, A3, A4, A5, A6, A7, A8> curry (
            final Function8<R, A1, A2, A3, A4, A5, A6, A7, A8> f,
            final A1 a1) {
        return new Function7<R, A2, A3, A4, A5, A6, A7, A8> () {
            public R run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function0<R> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8,
            final A9 a9) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function1<R, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8) {
        return new Function1<R, A9> () {
            public R run(A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function2<R, A8, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Function2<R, A8, A9> () {
            public R run(A8 a8, A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function3<R, A7, A8, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Function3<R, A7, A8, A9> () {
            public R run(A7 a7, A8 a8, A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function4<R, A6, A7, A8, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Function4<R, A6, A7, A8, A9> () {
            public R run(A6 a6, A7 a7, A8 a8, A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function5<R, A5, A6, A7, A8, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Function5<R, A5, A6, A7, A8, A9> () {
            public R run(A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function6<R, A4, A5, A6, A7, A8, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function6<R, A4, A5, A6, A7, A8, A9> () {
            public R run(A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function7<R, A3, A4, A5, A6, A7, A8, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1,
            final A2 a2) {
        return new Function7<R, A3, A4, A5, A6, A7, A8, A9> () {
            public R run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9> Function8<R, A2, A3, A4, A5, A6, A7, A8, A9> curry (
            final Function9<R, A1, A2, A3, A4, A5, A6, A7, A8, A9> f,
            final A1 a1) {
        return new Function8<R, A2, A3, A4, A5, A6, A7, A8, A9> () {
            public R run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function0<R> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8,
            final A9 a9,
            final A10 a10) {
        return new Function0<R> () {
            public R run() {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function1<R, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8,
            final A9 a9) {
        return new Function1<R, A10> () {
            public R run(A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function2<R, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7,
            final A8 a8) {
        return new Function2<R, A9, A10> () {
            public R run(A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function3<R, A8, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6,
            final A7 a7) {
        return new Function3<R, A8, A9, A10> () {
            public R run(A8 a8, A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function4<R, A7, A8, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5,
            final A6 a6) {
        return new Function4<R, A7, A8, A9, A10> () {
            public R run(A7 a7, A8 a8, A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function5<R, A6, A7, A8, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4,
            final A5 a5) {
        return new Function5<R, A6, A7, A8, A9, A10> () {
            public R run(A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function6<R, A5, A6, A7, A8, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3,
            final A4 a4) {
        return new Function6<R, A5, A6, A7, A8, A9, A10> () {
            public R run(A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function7<R, A4, A5, A6, A7, A8, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2,
            final A3 a3) {
        return new Function7<R, A4, A5, A6, A7, A8, A9, A10> () {
            public R run(A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function8<R, A3, A4, A5, A6, A7, A8, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1,
            final A2 a2) {
        return new Function8<R, A3, A4, A5, A6, A7, A8, A9, A10> () {
            public R run(A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }

    public static <R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> Function9<R, A2, A3, A4, A5, A6, A7, A8, A9, A10> curry (
            final Function10<R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10> f,
            final A1 a1) {
        return new Function9<R, A2, A3, A4, A5, A6, A7, A8, A9, A10> () {
            public R run(A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8, A9 a9, A10 a10) {
                return f.run(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
            }
        };
    }
}
