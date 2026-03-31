import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Store } from '@ngrx/store';
import { ProgressBarModule } from 'primeng/progressbar';
import { selectNotification } from '../../store/s3.selectors';

@Component({
  selector: 'app-notification-bar',
  standalone: true,
  imports: [CommonModule, ProgressBarModule],
  template: `
    @if (notification$ | async; as n) {
      <div class="notification-bar" [class]="'notif-' + n.type">
        <i [class]="n.type === 'info' ? 'pi pi-spin pi-spinner' : n.type === 'success' ? 'pi pi-check-circle' : 'pi pi-exclamation-circle'"></i>
        <span class="notif-msg">{{ n.message }}</span>
        @if (n.detail) {
          <span class="notif-detail">{{ n.detail }}</span>
        }
        @if (n.progress) {
          @if (n.progress.mode === 'determinate') {
            <span class="notif-pct">{{ n.progress.value }}%</span>
          }
          <div class="notif-progress">
            <p-progressBar
              [mode]="n.progress.mode"
              [value]="n.progress.value ?? 0"
              [showValue]="false"
              [style]="{ height: '3px', width: '80px' }"
            />
          </div>
        }
      </div>
    }
  `,
  styles: [`
    .notification-bar {
      position: fixed;
      top: 0.75rem;
      right: 0.75rem;
      z-index: 9999;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.5rem 1rem;
      border-radius: 8px;
      font-size: 0.85rem;
      white-space: nowrap;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    .notif-info { background: var(--p-blue-50); color: var(--p-blue-700); border: 1px solid var(--p-blue-200); }
    .notif-success { background: var(--p-green-50); color: var(--p-green-700); border: 1px solid var(--p-green-200); }
    .notif-error { background: var(--p-red-50); color: var(--p-red-700); border: 1px solid var(--p-red-200); }
    .notif-msg { font-weight: 600; }
    .notif-detail { opacity: 0.8; }
    .notif-pct { font-weight: 600; }
    .notif-progress { display: flex; align-items: center; }
  `]
})
export class NotificationBarComponent {
  notification$ = inject(Store).select(selectNotification);
}
