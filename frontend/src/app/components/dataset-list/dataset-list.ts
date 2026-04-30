import { Component, OnInit, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { TableModule } from 'primeng/table';
import { ButtonModule } from 'primeng/button';
import { TagModule } from 'primeng/tag';
import { ConfirmDialogModule } from 'primeng/confirmdialog';
import { DialogModule } from 'primeng/dialog';
import { FileUploadModule } from 'primeng/fileupload';
import { InputTextModule } from 'primeng/inputtext';
import { FloatLabelModule } from 'primeng/floatlabel';
import { Popover, PopoverModule } from 'primeng/popover';
import { ConfirmationService } from 'primeng/api';
import { DatasetActions } from '../../store/dataset.actions';
import { selectDatasets, selectListLoading, selectDuckdbSizeBytes, selectUploading, selectUploadDone } from '../../store/dataset.selectors';
import { Dataset } from '../../models/dataset.model';

@Component({
  selector: 'app-dataset-list',
  standalone: true,
  imports: [
    CommonModule, FormsModule, TableModule, ButtonModule, TagModule,
    ConfirmDialogModule, DialogModule, FileUploadModule, InputTextModule, FloatLabelModule,
    PopoverModule
  ],
  providers: [ConfirmationService],
  template: `
    <div class="dataset-list">
      <div class="title-bar">
        <h2>Datasets</h2>
        @if (duckdbSize$ | async; as size) {
          @if (size > 0) {
            <span class="db-size">DuckDB: {{ formatSize(size) }}</span>
          }
        }
        <div class="spacer"></div>
        <p-button label="Compare" icon="pi pi-arrows-h" [outlined]="true" (onClick)="goToCompare()" />
        <p-button label="Upload File" icon="pi pi-upload" [outlined]="true" (onClick)="showUploadDialog = true" />
        <p-button label="Import from S3" icon="pi pi-cloud-download" (onClick)="goToS3()" />
      </div>
      <p-table [value]="(datasets$ | async) ?? []" [loading]="(loading$ | async) ?? false" styleClass="p-datatable-sm p-datatable-striped">
        <ng-template #header>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Source</th>
            <th>Type</th>
            <th>Entity Path</th>
            <th>Run</th>
            <th>Rows</th>
            <th>Size</th>
            <th>Imported</th>
            <th>Actions</th>
          </tr>
        </ng-template>
        <ng-template #body let-ds>
          <tr>
            <td>{{ ds.id }}</td>
            <td>
              <a (click)="openDataset(ds)" class="dataset-link">{{ ds.name }}</a>
            </td>
            <td class="truncate" [title]="ds.sourceUri">{{ ds.sourceUri }}</td>
            <td><p-tag [value]="ds.sourceType" severity="info" /></td>
            <td>{{ ds.entityPath || '-' }}</td>
            <td>{{ ds.runTimestamp || '-' }}</td>
            <td>{{ ds.rowCount | number }}</td>
            <td>{{ formatSize(ds.sizeBytes) }}</td>
            <td>{{ ds.importedAt | date:'short' }}</td>
            <td>
              <p-button icon="pi pi-eye" [rounded]="true" [text]="true" (onClick)="openDataset(ds)" />
              <p-button icon="pi pi-pencil" [rounded]="true" [text]="true" (onClick)="openRename($event, ds)" />
              <p-button icon="pi pi-trash" [rounded]="true" [text]="true" severity="danger" (onClick)="confirmDelete(ds)" />
            </td>
          </tr>
        </ng-template>
        <ng-template #emptymessage>
          <tr><td colspan="10" class="text-center">No datasets imported yet.</td></tr>
        </ng-template>
      </p-table>
    </div>
    <p-confirmDialog />

    <p-popover #renamePopover>
      <div class="rename-popover">
        <p-floatlabel>
          <input
            #renameInput
            pInputText
            id="renameName"
            [(ngModel)]="renameName"
            (keydown.enter)="saveRename()"
            (keydown.escape)="renamePopover.hide()"
            class="rename-input"
          />
          <label for="renameName">Dataset name</label>
        </p-floatlabel>
        <div class="rename-actions">
          <p-button label="Cancel" [text]="true" size="small" (onClick)="renamePopover.hide()" />
          <p-button label="Save" icon="pi pi-check" size="small" (onClick)="saveRename()" [disabled]="!renameName.trim()" />
        </div>
      </div>
    </p-popover>

    <!-- Upload dialog -->
    <p-dialog header="Upload Files" [(visible)]="showUploadDialog" [modal]="true" [style]="{ width: '600px' }">
      <div class="upload-dialog">
        <div
          class="drop-zone"
          [class.drag-over]="isDragOver"
          (dragover)="onDragOver($event)"
          (dragleave)="isDragOver = false"
          (drop)="onDrop($event)"
        >
          <input #fileInput type="file" accept=".parquet,.csv,.csv.gz,.zip" multiple (change)="onFileSelected($event)" hidden />
          <input #dirInput type="file" webkitdirectory (change)="onDirSelected($event)" hidden />
          @if (uploadFiles.length) {
            <div class="files-info">
              <i class="pi pi-folder-open"></i>
              <span><strong>{{ uploadFiles.length }}</strong> file{{ uploadFiles.length === 1 ? '' : 's' }} · {{ formatSize(totalSize) }}</span>
            </div>
            <ul class="file-list">
              @for (f of uploadFiles.slice(0, 5); track $index) {
                <li>{{ fileLabel(f) }} <span class="muted">({{ formatSize(f.size) }})</span></li>
              }
              @if (uploadFiles.length > 5) {
                <li class="muted">…and {{ uploadFiles.length - 5 }} more</li>
              }
            </ul>
          } @else {
            <div class="drop-hint">
              <i class="pi pi-cloud-upload" style="font-size: 2rem"></i>
              <p>Drop files or a folder here</p>
              <p class="muted">.parquet, .csv, .csv.gz, .zip</p>
            </div>
          }
          <div class="browse-buttons" (click)="$event.stopPropagation()">
            <p-button label="Choose files" icon="pi pi-file" [outlined]="true" size="small" (onClick)="fileInput.click()" />
            <p-button label="Choose folder" icon="pi pi-folder" [outlined]="true" size="small" (onClick)="dirInput.click()" />
            @if (uploadFiles.length) {
              <p-button label="Clear" icon="pi pi-times" [text]="true" size="small" (onClick)="clearFiles()" />
            }
          </div>
        </div>

        @if (uploadFiles.length) {
          <div class="upload-name">
            <p-floatlabel>
              <input pInputText id="uploadName" [(ngModel)]="uploadName" style="width: 100%" />
              <label for="uploadName">Dataset name</label>
            </p-floatlabel>
          </div>
        }

        <div class="upload-footer">
          <p-button
            label="Upload"
            icon="pi pi-upload"
            (onClick)="doUpload()"
            [disabled]="!uploadFiles.length || !uploadName"
            [loading]="(uploading$ | async) ?? false"
          />
        </div>
      </div>
    </p-dialog>
  `,
  styles: [`
    .dataset-link { cursor: pointer; color: var(--p-primary-color); text-decoration: underline; }
    .truncate { max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .text-center { text-align: center; }
    .title-bar { display: flex; align-items: center; gap: 1rem; }
    .spacer { flex: 1; }
    .db-size { font-size: 0.85rem; color: var(--p-text-muted-color); padding: 0.25rem 0.75rem; border-radius: 6px; background: var(--p-surface-card); border: 1px solid var(--p-surface-border); }
    .upload-dialog { display: flex; flex-direction: column; gap: 1rem; }
    .drop-zone {
      border: 2px dashed var(--p-surface-border); border-radius: 8px;
      padding: 1.5rem; text-align: center;
      transition: border-color 0.2s, background 0.2s;
    }
    .drop-zone.drag-over {
      border-color: var(--p-primary-color); background: var(--p-primary-50);
    }
    .drop-hint { color: var(--p-text-muted-color); }
    .drop-hint p { margin: 0.5rem 0 0; font-size: 0.9rem; }
    .files-info { display: flex; align-items: center; gap: 0.75rem; justify-content: center; }
    .file-list { list-style: none; padding: 0; margin: 0.75rem 0 0; text-align: left; max-height: 160px; overflow: auto; font-size: 0.85rem; }
    .file-list li { padding: 0.15rem 0; }
    .muted { color: var(--p-text-muted-color); font-size: 0.85rem; }
    .browse-buttons { display: flex; justify-content: center; gap: 0.5rem; margin-top: 1rem; flex-wrap: wrap; }
    .upload-name { margin-top: 0.5rem; }
    .upload-footer { display: flex; justify-content: flex-end; }
    .rename-popover { display: flex; flex-direction: column; gap: 1rem; padding: 0.75rem 0.5rem; min-width: 520px; }
    .rename-input { width: 100%; font-size: 1rem; padding: 0.65rem 0.75rem; }
    .rename-actions { display: flex; justify-content: flex-end; gap: 0.5rem; }
  `]
})
export class DatasetListComponent implements OnInit {
  private store = inject(Store);
  private router = inject(Router);
  private confirmationService = inject(ConfirmationService);

  datasets$ = this.store.select(selectDatasets);
  loading$ = this.store.select(selectListLoading);
  duckdbSize$ = this.store.select(selectDuckdbSizeBytes);
  uploading$ = this.store.select(selectUploading);
  uploadDone$ = this.store.select(selectUploadDone);

  @ViewChild('renamePopover') renamePopover!: Popover;

  showUploadDialog = false;
  uploadFiles: File[] = [];
  uploadName = '';
  totalSize = 0;
  isDragOver = false;
  renameName = '';
  private renamingId: number | null = null;

  constructor() {
    // Close dialog when upload completes — single justified subscription for UI side effect
    this.uploadDone$.subscribe(done => {
      if (done && this.showUploadDialog) {
        this.showUploadDialog = false;
        this.clearFiles();
      }
    });
  }

  ngOnInit() {
    this.store.dispatch(DatasetActions.loadDatasets());
    this.store.dispatch(DatasetActions.loadHealth());
  }

  openDataset(ds: Dataset) {
    this.router.navigate(['/datasets', ds.id]);
  }

  goToCompare() {
    this.router.navigate(['/compare']);
  }

  goToS3() {
    this.router.navigate(['/s3']);
  }

  formatSize(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
  }

  openRename(event: Event, ds: Dataset) {
    this.renameName = ds.name;
    this.renamingId = ds.id;
    this.renamePopover.toggle(event);
  }

  saveRename() {
    const name = this.renameName.trim();
    if (!name || this.renamingId == null) return;
    this.store.dispatch(DatasetActions.renameDataset({ id: this.renamingId, name }));
    this.renamePopover.hide();
  }

  confirmDelete(ds: Dataset) {
    this.confirmationService.confirm({
      message: `Delete dataset "${ds.name}"?`,
      accept: () => {
        this.store.dispatch(DatasetActions.deleteDataset({ id: ds.id }));
      }
    });
  }

  // Upload
  onFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    if (input.files?.length) {
      this.setFiles(Array.from(input.files));
    }
    input.value = '';
  }

  onDirSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    if (input.files?.length) {
      const all = Array.from(input.files);
      const folderName = (all[0] as any).webkitRelativePath?.split('/')[0] ?? null;
      this.setFiles(all, folderName);
    }
    input.value = '';
  }

  onDragOver(event: DragEvent) {
    event.preventDefault();
    this.isDragOver = true;
  }

  async onDrop(event: DragEvent) {
    event.preventDefault();
    this.isDragOver = false;
    const dt = event.dataTransfer;
    if (!dt) return;

    const items = dt.items;
    const entries: FileSystemEntry[] = [];
    if (items?.length) {
      for (let i = 0; i < items.length; i++) {
        const entry = (items[i] as any).webkitGetAsEntry?.();
        if (entry) entries.push(entry);
      }
    }

    if (entries.length) {
      const collected: File[] = [];
      for (const entry of entries) await this.collectEntry(entry, collected);
      const folderName = (entries.length === 1 && entries[0].isDirectory) ? entries[0].name : null;
      if (collected.length) this.setFiles(collected, folderName);
    } else if (dt.files.length) {
      this.setFiles(Array.from(dt.files));
    }
  }

  private async collectEntry(entry: FileSystemEntry, out: File[]): Promise<void> {
    if (entry.isFile) {
      const file: File = await new Promise(res => (entry as FileSystemFileEntry).file(res));
      if (this.isSupported(file.name)) out.push(file);
    } else if (entry.isDirectory) {
      const reader = (entry as FileSystemDirectoryEntry).createReader();
      // readEntries returns batches of up to 100 — loop until empty
      while (true) {
        const batch: FileSystemEntry[] = await new Promise(res => reader.readEntries(es => res(es)));
        if (!batch.length) break;
        for (const child of batch) await this.collectEntry(child, out);
      }
    }
  }

  private isSupported(name: string): boolean {
    const n = name.toLowerCase();
    return n.endsWith('.parquet') || n.endsWith('.snappy.parquet')
      || n.endsWith('.csv') || n.endsWith('.csv.gz')
      || n.endsWith('.zip');
  }

  fileLabel(f: File): string {
    return (f as any).webkitRelativePath || f.name;
  }

  clearFiles() {
    this.uploadFiles = [];
    this.totalSize = 0;
    this.uploadName = '';
  }

  private setFiles(files: File[], folderName: string | null = null) {
    const filtered = files.filter(f => this.isSupported(f.name));
    if (!filtered.length) return;
    this.uploadFiles = filtered;
    this.totalSize = filtered.reduce((s, f) => s + f.size, 0);

    const ts = new Date(filtered[0].lastModified).toISOString().replace(/[:.]/g, '-').substring(0, 19);
    let displayName: string;
    if (folderName) {
      displayName = folderName;
    } else if (filtered.length === 1) {
      const f = filtered[0];
      displayName = f.name.replace(/\.(snappy\.)?parquet$|\.csv(\.gz)?$|\.zip$/i, '');
      // part-NNNNN-UUID Spark/parquet shard → use containing folder if known
      if (/^part-\d{5}-[0-9a-f]{8}/i.test(f.name)) {
        const rel = (f as any).webkitRelativePath as string | undefined;
        if (rel) {
          const parts = rel.split('/');
          if (parts.length >= 2) displayName = parts[parts.length - 2];
        }
      }
    } else {
      const rels = filtered.map(f => (f as any).webkitRelativePath as string).filter(Boolean);
      displayName = rels.length ? rels[0].split('/')[0] : `${filtered.length} files`;
    }
    this.uploadName = `${displayName} @ ${ts}`;
  }

  doUpload() {
    if (!this.uploadFiles.length || !this.uploadName) return;
    this.store.dispatch(DatasetActions.uploadFile({ files: this.uploadFiles, name: this.uploadName }));
  }
}
