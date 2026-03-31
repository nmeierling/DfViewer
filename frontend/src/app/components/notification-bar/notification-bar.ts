import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ProgressBarModule } from 'primeng/progressbar';
import { ButtonModule } from 'primeng/button';
import { NotificationService, Notification } from '../../services/notification.service';

@Component({
  selector: 'app-notification-bar',
  standalone: true,
  imports: [CommonModule, ProgressBarModule, ButtonModule],
  template: `
    <div class="notification-container">
      @for (n of notifications; track n.id) {
        <div class="notification-item" [class]="'notification-' + n.type">
          <div class="notification-content">
            <div class="notification-text">
              <span class="notification-message">{{ n.message }}</span>
              @if (n.detail) {
                <span class="notification-detail">{{ n.detail }}</span>
              }
            </div>
            <p-button icon="pi pi-times" [rounded]="true" [text]="true" size="small" (onClick)="dismiss(n.id)" />
          </div>
          @if (n.progress) {
            <p-progressBar
              [mode]="n.progress.mode"
              [value]="n.progress.value ?? 0"
              [showValue]="false"
              [style]="{ height: '4px' }"
            />
          }
        </div>
      }
    </div>
  `,
  styles: [`
    .notification-container {
      position: fixed;
      top: 1rem;
      right: 1rem;
      z-index: 9999;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
      max-width: 400px;
      min-width: 320px;
    }
    .notification-item {
      border-radius: 8px;
      overflow: hidden;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
      animation: slideIn 0.2s ease-out;
    }
    .notification-content {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      padding: 0.75rem 1rem;
      gap: 0.5rem;
    }
    .notification-text {
      display: flex;
      flex-direction: column;
      gap: 0.15rem;
      flex: 1;
      min-width: 0;
    }
    .notification-message { font-weight: 600; font-size: 0.9rem; }
    .notification-detail { font-size: 0.8rem; opacity: 0.85; }
    .notification-info {
      background: var(--p-surface-card);
      border: 1px solid var(--p-primary-color);
      color: var(--p-text-color);
    }
    .notification-success {
      background: var(--p-surface-card);
      border: 1px solid var(--p-green-500);
      color: var(--p-text-color);
    }
    .notification-error {
      background: var(--p-surface-card);
      border: 1px solid var(--p-red-500);
      color: var(--p-text-color);
    }
    @keyframes slideIn {
      from { transform: translateX(100%); opacity: 0; }
      to { transform: translateX(0); opacity: 1; }
    }
  `]
})
export class NotificationBarComponent {
  notifications: Notification[] = [];

  constructor(private notificationService: NotificationService) {
    this.notificationService.notifications.subscribe(n => this.notifications = n);
  }

  dismiss(id: string) {
    this.notificationService.dismiss(id);
  }
}
