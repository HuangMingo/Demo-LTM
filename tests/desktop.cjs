// Smoke test hai Electron instance ẩn chạy từ repo, bao gồm UI, IPC và backend Java thật.
// Chỉ giả lập hộp thoại trong test để số liệu phiên có thể lưu CSV và host đóng tự động.
const { _electron: electron } = require('playwright');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const net = require('node:net');
const { once } = require('node:events');
const runtime = require('../scripts/runtime.cjs');
async function unused() { const s=net.createServer(); s.listen(0,'0.0.0.0'); await once(s,'listening'); const p=s.address().port; await new Promise(r=>s.close(r)); return p; }
(async () => {
  const apps=[]; const directory=await fs.mkdtemp(path.join(os.tmpdir(),'relay-desktop-'));
  const restPort=await unused(), grpcPort=await unused();
  try {
    async function open(name) {
      const env={...process.env,RELAY_EXTERNAL_HOST:'',RELAY_SMOKE:'1'}; delete env.ELECTRON_RUN_AS_NODE;
      const instance=await electron.launch({ executablePath:runtime.dependencies(), args:[runtime.root,`--user-data-dir=${path.join(directory,name)}`], env });
      apps.push(instance); const page=await instance.firstWindow();
      await page.waitForFunction(()=>window.relay && !document.getElementById('connect').disabled);
      await page.locator('#restPort').fill(String(restPort)); await page.locator('#grpcPort').fill(String(grpcPort));
      await page.locator('#room').fill('desktop'); await page.locator('#username').fill(name);
      return {instance,page};
    }
    const host=await open('Host'); await host.page.locator('#role').selectOption('host'); await host.page.locator('#connect').click();
    await host.page.waitForFunction(()=>document.getElementById('status').textContent.includes('REST ·'),{},{timeout:30000});
    const client=await open('Guest'); await client.page.locator('#grpc').click(); await client.page.locator('#connect').click();
    await client.page.waitForFunction(()=>document.getElementById('status').textContent.includes('GRPC ·'));
    await host.page.locator('#text').fill('Desktop REST → gRPC ✓'); await host.page.locator('#text').press('Enter');
    await client.page.getByText('Desktop REST → gRPC ✓',{exact:true}).waitFor();
    await host.page.waitForFunction(()=>document.activeElement?.id === 'text');
    await client.page.locator('#text').fill('<img src=x onerror=alert(1)>'); await client.page.locator('#send').click();
    await host.page.getByText('<img src=x onerror=alert(1)>',{exact:true}).waitFor();
    assert.equal(await host.page.locator('#chatbox img').count(),0);
    await client.page.locator('#rest').click();
    await client.page.waitForFunction(()=>document.getElementById('status').textContent.startsWith('REST'));
    const ids=await client.page.locator('.bubble').count(); assert.equal(ids,2);
    assert.equal(await host.page.evaluate(()=>typeof window.require),'undefined');
    assert.equal(await client.page.evaluate(()=>typeof window.require),'undefined');
    // Xuất các phép đo từ cuộc trò chuyện thật, chỉ giả lập hộp thoại lưu trong test.
    await client.instance.evaluate(({dialog},output)=>{ dialog.showSaveDialog=async()=>({canceled:false,filePath:output}); },path.join(directory,'bench.csv'));
    await client.page.locator('#benchmark').click();
    await client.page.waitForFunction(()=>document.getElementById('benchmarkResult').textContent.includes('Lỗi: 0'),{},{timeout:120000});
    assert.match(await fs.readFile(path.join(directory,'bench.csv'),'utf8'),/median_ms,p95_ms/);
    await host.page.screenshot({path:path.join(runtime.root,'target','desktop-smoke.png')});
    await host.instance.evaluate(({dialog})=>{dialog.showMessageBoxSync=()=>1;});
    await host.page.evaluate(()=>window.close());
    await new Promise(r=>setTimeout(r,700));
    await assert.rejects(runtime.health('127.0.0.1',{restPort,grpcPort}));
    console.log('PASS Electron host/client, REST↔gRPC, safe text rendering, mode switch, session CSV, host close cleanup');
  } finally {
    for(const instance of apps.reverse()) { try { await instance.evaluate(({dialog})=>{dialog.showMessageBoxSync=()=>1;}); await instance.close(); } catch {} }
    await fs.rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:200});
  }
})().catch(e=>{console.error(e);process.exitCode=1;});
