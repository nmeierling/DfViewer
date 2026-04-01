import { Routes } from '@angular/router';
import { DatasetListComponent } from './components/dataset-list/dataset-list';
import { DataTableComponent } from './components/data-table/data-table';
import { S3BrowserComponent } from './components/s3-browser/s3-browser';
import { ComparisonComponent } from './components/comparison/comparison';

export const routes: Routes = [
  { path: '', component: DatasetListComponent },
  { path: 'datasets/:id', component: DataTableComponent },
  { path: 's3', component: S3BrowserComponent },
  { path: 'compare', component: ComparisonComponent },
];
