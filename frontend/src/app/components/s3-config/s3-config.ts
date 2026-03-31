import { Component, OnInit, OnDestroy, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { InputTextModule } from 'primeng/inputtext';
import { TextareaModule } from 'primeng/textarea';
import { ButtonModule } from 'primeng/button';
import { FloatLabelModule } from 'primeng/floatlabel';
import { MessageModule } from 'primeng/message';
import { S3Service } from '../../services/s3.service';
import { S3Credentials } from '../../models/s3.model';

const LS_CREDENTIALS = 'dfviewer_s3_credentials';
const LS_SCAN_URI = 'dfviewer_s3_scan_uri';

@Component({
  selector: 'app-s3-config',
  standalone: true,
  imports: [CommonModule, FormsModule, InputTextModule, TextareaModule, ButtonModule, FloatLabelModule, MessageModule],
  template: `
    <div class="s3-config">
      <h3>S3 Credentials</h3>

      @if (tokenExpired) {
        <p-message severity="error" text="AWS credentials have expired. Please reconfigure." />
      }

      @if (configured && !tokenExpired) {
        <p-message severity="success" text="S3 configured (region: {{ region }})" />
        <p-button label="Reconfigure" [text]="true" icon="pi pi-pencil" (onClick)="showForm()" />
      } @else {
        <div class="config-form">
          <div class="field">
            <label for="pasteArea" class="paste-label">Paste environment variables</label>
            <textarea
              pTextarea
              id="pasteArea"
              [(ngModel)]="pasteText"
              (ngModelChange)="parsePaste()"
              rows="5"
              placeholder="export AWS_ACCESS_KEY_ID=&quot;AKIA...&quot;&#10;export AWS_SECRET_ACCESS_KEY=&quot;...&quot;&#10;export AWS_SESSION_TOKEN=&quot;...&quot;&#10;export AWS_REGION=&quot;eu-central-1&quot;"
              style="width: 100%; font-family: monospace; font-size: 0.85rem"
            ></textarea>
            @if (parseMessage) {
              <small class="parse-message">{{ parseMessage }}</small>
            }
          </div>

          <div class="divider"><span>or fill manually</span></div>

          <div class="field">
            <p-floatlabel>
              <input pInputText id="accessKey" [(ngModel)]="credentials.accessKeyId" style="width: 100%" />
              <label for="accessKey">AWS Access Key ID</label>
            </p-floatlabel>
          </div>
          <div class="field">
            <p-floatlabel>
              <input pInputText id="secretKey" [(ngModel)]="credentials.secretAccessKey" type="password" style="width: 100%" />
              <label for="secretKey">AWS Secret Access Key</label>
            </p-floatlabel>
          </div>
          <div class="field">
            <p-floatlabel>
              <input pInputText id="sessionToken" [(ngModel)]="credentials.sessionToken" type="password" style="width: 100%" />
              <label for="sessionToken">AWS Session Token (optional)</label>
            </p-floatlabel>
          </div>
          <div class="field">
            <p-floatlabel>
              <input pInputText id="region" [(ngModel)]="credentials.region" style="width: 100%" />
              <label for="region">Region</label>
            </p-floatlabel>
          </div>
          <p-button label="Configure" icon="pi pi-check" (onClick)="save()" [loading]="saving" />
        </div>
      }
    </div>
  `,
  styles: [`
    .config-form { display: flex; flex-direction: column; gap: 1.5rem; max-width: 500px; }
    .field { width: 100%; }
    .s3-config { margin-bottom: 1.5rem; }
    .paste-label { font-weight: 500; margin-bottom: 0.5rem; display: block; }
    .parse-message { color: var(--p-primary-color); }
    .divider {
      display: flex; align-items: center; gap: 0.75rem; color: var(--p-text-muted-color); font-size: 0.85rem;
      &::before, &::after { content: ''; flex: 1; border-top: 1px solid var(--p-surface-border); }
    }
  `]
})
export class S3ConfigComponent implements OnInit, OnDestroy {
  @Output() configuredChange = new EventEmitter<boolean>();
  private tokenExpiredHandler = () => this.markTokenExpired();

  credentials: S3Credentials = {
    accessKeyId: '',
    secretAccessKey: '',
    sessionToken: '',
    region: 'eu-central-1'
  };
  configured = false;
  tokenExpired = false;
  region = '';
  saving = false;
  pasteText = '';
  parseMessage = '';

  constructor(private s3Service: S3Service) {}

  ngOnInit() {
    window.addEventListener('s3-token-expired', this.tokenExpiredHandler);

    // Try to load saved credentials from localStorage
    const saved = localStorage.getItem(LS_CREDENTIALS);
    if (saved) {
      try {
        this.credentials = JSON.parse(saved);
      } catch { /* ignore */ }
    }

    // Check backend status first
    this.s3Service.getStatus().subscribe(status => {
      if (status.configured) {
        this.configured = true;
        this.region = status.region;
        this.configuredChange.emit(true);
      } else if (this.credentials.accessKeyId && this.credentials.secretAccessKey) {
        // Backend not configured but we have saved credentials — auto-send them
        this.save();
      }
    });
  }

  ngOnDestroy() {
    window.removeEventListener('s3-token-expired', this.tokenExpiredHandler);
  }

  showForm() {
    this.configured = false;
    this.tokenExpired = false;
  }

  markTokenExpired() {
    this.tokenExpired = true;
    this.configured = false;
    this.configuredChange.emit(false);
  }

  parsePaste() {
    if (!this.pasteText.trim()) {
      this.parseMessage = '';
      return;
    }

    const vars = this.parseEnvVars(this.pasteText);
    let found = 0;

    if (vars['AWS_ACCESS_KEY_ID']) { this.credentials.accessKeyId = vars['AWS_ACCESS_KEY_ID']; found++; }
    if (vars['AWS_SECRET_ACCESS_KEY']) { this.credentials.secretAccessKey = vars['AWS_SECRET_ACCESS_KEY']; found++; }
    if (vars['AWS_SESSION_TOKEN']) { this.credentials.sessionToken = vars['AWS_SESSION_TOKEN']; found++; }
    if (vars['AWS_REGION'] || vars['AWS_DEFAULT_REGION']) {
      this.credentials.region = vars['AWS_REGION'] || vars['AWS_DEFAULT_REGION'];
      found++;
    }

    this.parseMessage = found > 0 ? `Parsed ${found} variable${found > 1 ? 's' : ''}` : 'No AWS variables found';
  }

  private parseEnvVars(text: string): Record<string, string> {
    const result: Record<string, string> = {};

    for (const line of text.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;

      const match = trimmed.match(/^(?:export\s+)?(\w+)=(?:"([^"]*)"|'([^']*)'|(.*))/);
      if (match) {
        const key = match[1];
        const value = (match[2] ?? match[3] ?? match[4] ?? '').trim();
        result[key] = value;
      }
    }

    return result;
  }

  save() {
    this.saving = true;
    this.s3Service.configure(this.credentials).subscribe({
      next: () => {
        this.configured = true;
        this.tokenExpired = false;
        this.region = this.credentials.region;
        this.saving = false;
        this.configuredChange.emit(true);
        // Persist to localStorage
        localStorage.setItem(LS_CREDENTIALS, JSON.stringify(this.credentials));
      },
      error: () => { this.saving = false; }
    });
  }

  static getSavedScanUri(): string {
    return localStorage.getItem(LS_SCAN_URI) || '';
  }

  static saveScanUri(uri: string) {
    localStorage.setItem(LS_SCAN_URI, uri);
  }
}
