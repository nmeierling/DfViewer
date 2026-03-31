import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { TableModule } from 'primeng/table';
import { ButtonModule } from 'primeng/button';
import { TagModule } from 'primeng/tag';
import { ConfirmDialogModule } from 'primeng/confirmdialog';
import { ConfirmationService } from 'primeng/api';
import { DatasetActions } from '../../store/dataset.actions';
import { selectDatasets, selectListLoading } from '../../store/dataset.selectors';
import { Dataset } from '../../models/dataset.model';

@Component({
  selector: 'app-dataset-list',
  standalone: true,
  imports: [CommonModule, TableModule, ButtonModule, TagModule, ConfirmDialogModule],
  providers: [ConfirmationService],
  template: `
    <div class="dataset-list">
      <div class="title-bar">
        <h2>Datasets</h2>
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
            <td>{{ ds.importedAt | date:'short' }}</td>
            <td>
              <p-button icon="pi pi-eye" [rounded]="true" [text]="true" (onClick)="openDataset(ds)" />
              <p-button icon="pi pi-trash" [rounded]="true" [text]="true" severity="danger" (onClick)="confirmDelete(ds)" />
            </td>
          </tr>
        </ng-template>
        <ng-template #emptymessage>
          <tr><td colspan="9" class="text-center">No datasets imported yet.</td></tr>
        </ng-template>
      </p-table>
    </div>
    <p-confirmDialog />
  `,
  styles: [`
    .dataset-link { cursor: pointer; color: var(--p-primary-color); text-decoration: underline; }
    .truncate { max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .text-center { text-align: center; }
    .title-bar { display: flex; align-items: center; justify-content: space-between; }
  `]
})
export class DatasetListComponent implements OnInit {
  private store = inject(Store);
  private router = inject(Router);
  private confirmationService = inject(ConfirmationService);

  datasets$ = this.store.select(selectDatasets);
  loading$ = this.store.select(selectListLoading);

  ngOnInit() {
    this.store.dispatch(DatasetActions.loadDatasets());
  }

  openDataset(ds: Dataset) {
    this.router.navigate(['/datasets', ds.id]);
  }

  goToS3() {
    this.router.navigate(['/s3']);
  }

  confirmDelete(ds: Dataset) {
    this.confirmationService.confirm({
      message: `Delete dataset "${ds.name}"?`,
      accept: () => {
        this.store.dispatch(DatasetActions.deleteDataset({ id: ds.id }));
      }
    });
  }
}
