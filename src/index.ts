import { SyncCompositeRedactor, SyncCompositeRedactorOptions } from './SyncCompositeRedactor';
import { AsyncCompositeRedactor, AsyncCompositeRedactorOptions } from './AsyncCompositeRedactor';
import { GoogleDLPRedactor, GoogleDLPRedactorOptions } from './custom/GoogleDLPRedactor';
import { HDXGoogleDLPRedactor } from './custom/HDXGoogleDLPRedactor';
import {
  AsyncCustomRedactorConfig,
  CompositeRedactorOptions,
  IAsyncRedactor,
  ISyncRedactor,
  IRedactor,
  SimpleRegexpCustomRedactorConfig
} from './types';

export {
  SyncCompositeRedactor as SyncRedactor,
  SyncCompositeRedactorOptions,
  AsyncCompositeRedactor as AsyncRedactor,
  AsyncCompositeRedactorOptions,
  GoogleDLPRedactor,
  HDXGoogleDLPRedactor,
  GoogleDLPRedactorOptions,
  AsyncCustomRedactorConfig,
  CompositeRedactorOptions,
  IAsyncRedactor,
  ISyncRedactor,
  IRedactor,
  SimpleRegexpCustomRedactorConfig
};
