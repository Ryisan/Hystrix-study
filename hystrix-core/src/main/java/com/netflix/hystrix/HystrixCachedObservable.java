package com.netflix.hystrix;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.subjects.ReplaySubject;

/**
 * HystrixCachedObservable 是对传入Observable的一个封装
 * @param <R>
 */
public class HystrixCachedObservable<R> {
    protected final Subscription originalSubscription;
    protected final Observable<R> cachedObservable;
    private volatile int outstandingSubscriptions = 0;

    protected HystrixCachedObservable(final Observable<R> originalObservable) {
        //使用 ReplaySubject 向传入的 Observable 发起订阅，通过 ReplaySubject 能够重放执行结果，从而实现缓存的功效
        ReplaySubject<R> replaySubject = ReplaySubject.create();
        this.originalSubscription = originalObservable
                .subscribe(replaySubject);

        this.cachedObservable = replaySubject
                .doOnUnsubscribe(new Action0() {
                    @Override
                    public void call() {
                        outstandingSubscriptions--;
                        if (outstandingSubscriptions == 0) {
                            originalSubscription.unsubscribe();
                        }
                    }
                })
                .doOnSubscribe(new Action0() {
                    @Override
                    public void call() {
                        outstandingSubscriptions++;
                    }
                });
    }

    public static <R> HystrixCachedObservable<R> from(Observable<R> o, AbstractCommand<R> originalCommand) {
        return new HystrixCommandResponseFromCache<R>(o, originalCommand);
    }

    public static <R> HystrixCachedObservable<R> from(Observable<R> o) {
        return new HystrixCachedObservable<R>(o);
    }

    public Observable<R> toObservable() {
        return cachedObservable;
    }

    public void unsubscribe() {
        originalSubscription.unsubscribe();
    }
}
