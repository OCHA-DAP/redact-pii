## How to release new version
First login to NPM: `npm login`

Update the version in `package.json` and `npm build` to check.

Then: `npm run prettier && npm publish --access=public`
