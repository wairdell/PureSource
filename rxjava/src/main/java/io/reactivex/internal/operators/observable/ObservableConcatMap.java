/**
 * Copyright (c) 2016-present, RxJava Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package io.reactivex.internal.operators.observable;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;

import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.internal.disposables.*;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.fuseable.*;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.util.*;
import io.reactivex.observers.SerializedObserver;
import io.reactivex.plugins.RxJavaPlugins;

public final class ObservableConcatMap<T, U> extends AbstractObservableWithUpstream<T, U> {
    final Function<? super T, ? extends ObservableSource<? extends U>> mapper;
    final int bufferSize;

    final ErrorMode delayErrors;

    public ObservableConcatMap(ObservableSource<T> source, Function<? super T, ? extends ObservableSource<? extends U>> mapper,
            int bufferSize, ErrorMode delayErrors) {
        super(source);
        // source 上游
        // mapper 将上游的 emission 转为一个 source，并订阅这些 source
        this.mapper = mapper;
        this.delayErrors = delayErrors;//错误不中断其他 source？
        this.bufferSize = Math.max(8, bufferSize);// 缓存大小，最大为 8
    }

    @Override
    public void subscribeActual(Observer<? super U> observer) {

        if (ObservableScalarXMap.tryScalarXMapSubscribe(source, observer, mapper)) {
            return;
        }

        if (delayErrors == ErrorMode.IMMEDIATE) {
            //upstream => SourceObserver => SerializedObserver => downstream
            SerializedObserver<U> serial = new SerializedObserver<U>(observer);
            source.subscribe(new SourceObserver<T, U>(serial, mapper, bufferSize));
        } else {
            source.subscribe(new ConcatMapDelayErrorObserver<T, U>(observer, mapper, bufferSize, delayErrors == ErrorMode.END));
        }
    }

    static final class SourceObserver<T, U> extends AtomicInteger implements Observer<T>, Disposable {

        private static final long serialVersionUID = 8828587559905699186L;
        final Observer<? super U> downstream;
        final Function<? super T, ? extends ObservableSource<? extends U>> mapper;
        final InnerObserver<U> inner;
        final int bufferSize;

        SimpleQueue<T> queue;

        Disposable upstream;

        volatile boolean active;

        volatile boolean disposed;

        volatile boolean done;

        int fusionMode;

        SourceObserver(Observer<? super U> actual,
                                Function<? super T, ? extends ObservableSource<? extends U>> mapper, int bufferSize) {
            this.downstream = actual;
            this.mapper = mapper;
            this.bufferSize = bufferSize;
            this.inner = new InnerObserver<U>(actual, this);
        }

        @Override
        public void onSubscribe(Disposable d) {
            if (DisposableHelper.validate(this.upstream, d)) {
                this.upstream = d;
                if (d instanceof QueueDisposable) {
                    @SuppressWarnings("unchecked")
                    QueueDisposable<T> qd = (QueueDisposable<T>) d;

                    int m = qd.requestFusion(QueueDisposable.ANY);
                    if (m == QueueDisposable.SYNC) {
                        fusionMode = m;
                        queue = qd;
                        done = true;

                        downstream.onSubscribe(this);

                        drain();
                        return;
                    }

                    if (m == QueueDisposable.ASYNC) {
                        fusionMode = m;
                        queue = qd;

                        downstream.onSubscribe(this);

                        return;
                    }
                }

                queue = new SpscLinkedArrayQueue<T>(bufferSize);

                downstream.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            if (fusionMode == QueueDisposable.NONE) {
                queue.offer(t);
            }
            drain();
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                RxJavaPlugins.onError(t);
                return;
            }
            done = true;
            dispose();
            downstream.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            drain();
        }

        void innerComplete() {
            active = false;
            drain();
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }

        @Override
        public void dispose() {
            disposed = true;
            inner.dispose();
            upstream.dispose();

            if (getAndIncrement() == 0) {
                queue.clear();
            }
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            for (;;) {
                if (disposed) {
                    queue.clear();
                    return;
                }
                //使用 active 保证一个转换后的 Observable 订阅事件没结束前，不转换和订阅其他的 Observable
                if (!active) {

                    boolean d = done;

                    T t;

                    try {
                        t = queue.poll();
                    } catch (Throwable ex) {
                        Exceptions.throwIfFatal(ex);
                        dispose();
                        queue.clear();
                        downstream.onError(ex);
                        return;
                    }

                    boolean empty = t == null;

                    if (d && empty) {
                        disposed = true;
                        downstream.onComplete();
                        return;
                    }

                    if (!empty) {
                        ObservableSource<? extends U> o;

                        try {
                            o = ObjectHelper.requireNonNull(mapper.apply(t), "The mapper returned a null ObservableSource");
                        } catch (Throwable ex) {
                            Exceptions.throwIfFatal(ex);
                            dispose();
                            queue.clear();
                            downstream.onError(ex);
                            return;
                        }

                        active = true;
                        o.subscribe(inner);
                    }
                }

                if (decrementAndGet() == 0) {
                    break;
                }
            }
        }

        static final class InnerObserver<U> extends AtomicReference<Disposable> implements Observer<U> {

            private static final long serialVersionUID = -7449079488798789337L;

            final Observer<? super U> downstream;
            final SourceObserver<?, ?> parent;

            InnerObserver(Observer<? super U> actual, SourceObserver<?, ?> parent) {
                this.downstream = actual;
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Disposable d) {
                DisposableHelper.replace(this, d);
            }

            @Override
            public void onNext(U t) {
                downstream.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                parent.dispose();
                downstream.onError(t);
            }

            @Override
            public void onComplete() {
                parent.innerComplete();
            }

            void dispose() {
                DisposableHelper.dispose(this);
            }
        }
    }

    static final class ConcatMapDelayErrorObserver<T, R>
    extends AtomicInteger
    implements Observer<T>, Disposable {

        private static final long serialVersionUID = -6951100001833242599L;

        final Observer<? super R> downstream;

        final Function<? super T, ? extends ObservableSource<? extends R>> mapper;

        final int bufferSize;

        final AtomicThrowable error;

        final DelayErrorInnerObserver<R> observer;

        final boolean tillTheEnd;

        SimpleQueue<T> queue;

        Disposable upstream;

        volatile boolean active;

        volatile boolean done;

        volatile boolean cancelled;

        int sourceMode;

        ConcatMapDelayErrorObserver(Observer<? super R> actual,
                Function<? super T, ? extends ObservableSource<? extends R>> mapper, int bufferSize,
                        boolean tillTheEnd) {
            this.downstream = actual;
            this.mapper = mapper;
            this.bufferSize = bufferSize;
            this.tillTheEnd = tillTheEnd;
            this.error = new AtomicThrowable();
            this.observer = new DelayErrorInnerObserver<R>(actual, this);
        }

        @Override
        public void onSubscribe(Disposable d) {
            if (DisposableHelper.validate(this.upstream, d)) {
                this.upstream = d;

                if (d instanceof QueueDisposable) {
                    @SuppressWarnings("unchecked")
                    QueueDisposable<T> qd = (QueueDisposable<T>) d;

                    int m = qd.requestFusion(QueueDisposable.ANY);
                    if (m == QueueDisposable.SYNC) {
                        sourceMode = m;
                        queue = qd;
                        done = true;

                        downstream.onSubscribe(this);

                        drain();
                        return;
                    }
                    if (m == QueueDisposable.ASYNC) {
                        sourceMode = m;
                        queue = qd;

                        downstream.onSubscribe(this);

                        return;
                    }
                }

                queue = new SpscLinkedArrayQueue<T>(bufferSize);

                downstream.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T value) {
            if (sourceMode == QueueDisposable.NONE) {
                queue.offer(value);
            }
            drain();
        }

        @Override
        public void onError(Throwable e) {
            if (error.addThrowable(e)) {
                done = true;
                drain();
            } else {
                RxJavaPlugins.onError(e);
            }
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        @Override
        public boolean isDisposed() {
            return cancelled;
        }

        @Override
        public void dispose() {
            cancelled = true;
            upstream.dispose();
            observer.dispose();
        }

        @SuppressWarnings("unchecked")
        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            Observer<? super R> actual = this.downstream;
            SimpleQueue<T> queue = this.queue;
            AtomicThrowable error = this.error;

            for (;;) {

                if (!active) {

                    if (cancelled) {
                        queue.clear();
                        return;
                    }

                    if (!tillTheEnd) {
                        Throwable ex = error.get();
                        if (ex != null) {
                            queue.clear();
                            cancelled = true;
                            actual.onError(error.terminate());
                            return;
                        }
                    }

                    boolean d = done;

                    T v;

                    try {
                        v = queue.poll();
                    } catch (Throwable ex) {
                        Exceptions.throwIfFatal(ex);
                        cancelled = true;
                        this.upstream.dispose();
                        error.addThrowable(ex);
                        actual.onError(error.terminate());
                        return;
                    }

                    boolean empty = v == null;

                    if (d && empty) {
                        cancelled = true;
                        Throwable ex = error.terminate();
                        if (ex != null) {
                            actual.onError(ex);
                        } else {
                            actual.onComplete();
                        }
                        return;
                    }

                    if (!empty) {

                        ObservableSource<? extends R> o;

                        try {
                            o = ObjectHelper.requireNonNull(mapper.apply(v), "The mapper returned a null ObservableSource");
                        } catch (Throwable ex) {
                            Exceptions.throwIfFatal(ex);
                            cancelled = true;
                            this.upstream.dispose();
                            queue.clear();
                            error.addThrowable(ex);
                            actual.onError(error.terminate());
                            return;
                        }

                        if (o instanceof Callable) {
                            R w;

                            try {
                                w = ((Callable<R>)o).call();
                            } catch (Throwable ex) {
                                Exceptions.throwIfFatal(ex);
                                error.addThrowable(ex);
                                continue;
                            }

                            if (w != null && !cancelled) {
                                actual.onNext(w);
                            }
                            continue;
                        } else {
                            active = true;
                            o.subscribe(observer);
                        }
                    }
                }

                if (decrementAndGet() == 0) {
                    break;
                }
            }
        }

        static final class DelayErrorInnerObserver<R> extends AtomicReference<Disposable> implements Observer<R> {

            private static final long serialVersionUID = 2620149119579502636L;

            final Observer<? super R> downstream;

            final ConcatMapDelayErrorObserver<?, R> parent;

            DelayErrorInnerObserver(Observer<? super R> actual, ConcatMapDelayErrorObserver<?, R> parent) {
                this.downstream = actual;
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Disposable d) {
                DisposableHelper.replace(this, d);
            }

            @Override
            public void onNext(R value) {
                downstream.onNext(value);
            }

            @Override
            public void onError(Throwable e) {
                ConcatMapDelayErrorObserver<?, R> p = parent;
                if (p.error.addThrowable(e)) {
                    if (!p.tillTheEnd) {
                        p.upstream.dispose();
                    }
                    p.active = false;
                    p.drain();
                } else {
                    RxJavaPlugins.onError(e);
                }
            }

            @Override
            public void onComplete() {
                ConcatMapDelayErrorObserver<?, R> p = parent;
                p.active = false;
                p.drain();
            }

            void dispose() {
                DisposableHelper.dispose(this);
            }
        }
    }
}
