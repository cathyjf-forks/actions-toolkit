// SPDX-FileCopyrightText: Copyright 2019 GitHub
// SPDX-FileCopyrightText: Copyright 2026 Cathy J. Fitzpatrick <cathy@cathyjf.com>
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This file is derived from the GitHub Actions Toolkit:
// <https://github.com/actions/toolkit>.
//
// Cathy J. Fitzpatrick's copyright is asserted over her modifications
// and over the resulting combined work as a derivative work.
//
// This modified file, as a whole, is licensed under GPL-3.0-or-later.
// The original upstream code remains licensed under the MIT License.
// See this package's `LICENSE.GPL-3.0` and `LICENSE.md` for the full
// license texts and the original upstream copyright and license notice.

import {ArtifactClient, DefaultArtifactClient} from './internal/client.js'

export * from './internal/shared/interfaces.js'
export * from './internal/shared/errors.js'
export * from './internal/client.js'
export {uploadArtifactStream} from './internal/upload/upload-artifact.js'

const client: ArtifactClient = new DefaultArtifactClient()
export default client
