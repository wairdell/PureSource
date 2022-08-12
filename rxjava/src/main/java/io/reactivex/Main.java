package io.reactivex;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

/**
 * author : fengqiao
 * date   : 2022/8/12 9:20
 * desc   :
 */
public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        Observable.just("a", "b", "c", "d").concatMap(new Function<String, ObservableSource<String>>() {
            @Override
            public ObservableSource<String> apply(@NonNull String s) throws Exception {
                return Observable.create(new ObservableOnSubscribe<String>() {
                    @Override
                    public void subscribe(@NonNull ObservableEmitter<String> emitter) throws Exception {
                        executorService.execute(new Runnable() {
                            @Override
                            public void run() {
                                CountDownLatch countDownLatch = new CountDownLatch(7);
                                for (int i = 0; i < 7; i++) {
                                    int index = i;
                                    executorService.execute(new Runnable() {
                                        @Override
                                        public void run() {
                                            emitter.onNext(s + index);
                                            countDownLatch.countDown();
                                        }
                                    });
                                }
                                try {
                                    countDownLatch.await();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                } finally {
                                    emitter.onComplete();
                                }
                            }
                        });
                    }
                });
//                return Observable.just(s + "a", s + "b");
            }
        }).subscribe(new Observer<String>() {
            @Override
            public void onSubscribe(@NonNull Disposable d) {
                System.out.println("onSubscribe");
            }

            @Override
            public void onNext(@NonNull String s) {
                System.out.println("onNext =>" + s);
            }

            @Override
            public void onError(@NonNull Throwable e) {
                System.out.println("onError");
            }

            @Override
            public void onComplete() {
                System.out.println("onComplete");
            }
        });
//        Thread.sleep(1000);
    }

}
