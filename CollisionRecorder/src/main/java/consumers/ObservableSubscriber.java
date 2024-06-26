/** Daša Nosková - xnosko05
 *  VUT FIT 2024
 */

package consumers;


import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

public class ObservableSubscriber<T> implements Subscriber<T> {
    private Subscription subscription;

    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscription.request(1); // Request the first data item
    }

    @Override
    public void onNext(T t) {
        //System.out.println("Received: " + t);
        subscription.request(1); // Request the next data item
    }

    @Override
    public void onError(Throwable t) {
        System.out.println("Failed");
        t.printStackTrace();
    }

    @Override
    public void onComplete() {
        //System.out.println("Completed");
    }

}
