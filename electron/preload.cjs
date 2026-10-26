const { contextBridge, ipcRenderer } = require('electron');
// Cầu nối hẹp qua contextBridge: renderer không được truy cập Node hay ipcRenderer trực tiếp.
contextBridge.exposeInMainWorld('relay', {
  call: async (action, value) => {
    const allowed = ['init','connect','send','mode','upload','download','copy','benchmark'];
    if (!allowed.includes(action)) throw Error('Thao tác không hợp lệ.');
    const result = await ipcRenderer.invoke('relay:call', action, value);
    if (!result.ok) throw Error(result.error); return result.value;
  },
  onEvent: callback => { const listener = (_, payload) => callback(payload); ipcRenderer.on('relay:event', listener); return () => ipcRenderer.removeListener('relay:event', listener); }
});
