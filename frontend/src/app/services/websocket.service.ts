import { Injectable, NgZone } from '@angular/core';
import { Client, IMessage, StompSubscription } from '@stomp/stompjs';
import { Observable, ReplaySubject, BehaviorSubject, first, filter } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class WebSocketService {
  private client: Client;
  private connected$ = new BehaviorSubject<boolean>(false);

  constructor(private zone: NgZone) {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws`;

    this.client = new Client({
      brokerURL: wsUrl,
      reconnectDelay: 5000,
      connectionTimeout: 10000,
      onConnect: () => {
        console.log('[WS] Connected');
        this.zone.run(() => this.connected$.next(true));
      },
      onDisconnect: () => {
        console.log('[WS] Disconnected');
        this.zone.run(() => this.connected$.next(false));
      },
      onStompError: (frame) => {
        console.error('[WS] STOMP error', frame.headers?.['message'] || frame);
      },
      onWebSocketError: (event) => {
        console.warn('[WS] WebSocket error (backend may not be running yet)', event);
      }
    });

    this.client.activate();
  }

  /** Wait until WebSocket is connected, then resolve */
  whenConnected(): Observable<true> {
    return this.connected$.pipe(
      filter(c => c),
      first()
    ) as Observable<true>;
  }

  /**
   * Subscribe to a STOMP topic. Uses ReplaySubject to buffer messages
   * so nothing is lost even if the outer subscriber attaches slightly late.
   * Returns both the observable and a cleanup function.
   */
  subscribe<T>(topic: string): { messages$: Observable<T>; unsubscribe: () => void } {
    // ReplaySubject buffers all messages so late subscribers still get them
    const subject = new ReplaySubject<T>();
    let stompSub: StompSubscription | null = null;

    const doSubscribe = () => {
      console.log(`[WS] Subscribing to ${topic}`);
      stompSub = this.client.subscribe(topic, (message: IMessage) => {
        this.zone.run(() => {
          try {
            const parsed = JSON.parse(message.body) as T;
            console.log(`[WS] Message on ${topic}:`, (parsed as any)?.type || 'unknown');
            subject.next(parsed);
          } catch (e) {
            console.error(`[WS] Failed to parse message on ${topic}:`, e, message.body?.substring(0, 200));
          }
        });
      });
    };

    if (this.client.connected) {
      doSubscribe();
    } else {
      this.whenConnected().subscribe(() => doSubscribe());
    }

    return {
      messages$: subject.asObservable(),
      unsubscribe: () => {
        stompSub?.unsubscribe();
        subject.complete();
      }
    };
  }
}
