const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const staticDir = path.join(__dirname, '../static');
const iconsDir = path.join(staticDir, 'icons');

test('web pages declare the shared browser icons', () => {
  for (const page of ['index.html']) {
    const html = fs.readFileSync(path.join(staticDir, page), 'utf8');
    assert.match(html, /rel="icon" type="image\/svg\+xml" href="icons\/favicon\.svg"/);
    assert.match(html, /rel="icon" type="image\/png" sizes="32x32" href="icons\/favicon-32x32\.png"/);
    assert.match(html, /rel="alternate icon" href="icons\/favicon\.ico"/);
    assert.match(html, /rel="apple-touch-icon" sizes="180x180" href="icons\/apple-touch-icon\.png"/);
  }
});

test('generated raster icons use valid PNG and ICO signatures', () => {
  const pngSignature = Buffer.from('89504e470d0a1a0a', 'hex');
  for (const file of ['favicon-32x32.png', 'apple-touch-icon.png']) {
    const contents = fs.readFileSync(path.join(iconsDir, file));
    assert.deepEqual(contents.subarray(0, pngSignature.length), pngSignature);
  }

  const ico = fs.readFileSync(path.join(iconsDir, 'favicon.ico'));
  assert.deepEqual(ico.subarray(0, 4), Buffer.from([0, 0, 1, 0]));
});
