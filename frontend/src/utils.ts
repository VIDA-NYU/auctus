export function generateRandomId() {
  return Math.random()
    .toString(36)
    .substr(2, 9);
}

export function formatSize(size: number) {
  const units = ['bytes', 'kb', 'mb', 'gb', 'tb'];
  for (let i = 0; i < units.length; i++) {
    const unitMultiplier = Math.pow(1024, i);
    const unitMaximum = unitMultiplier * 1024;
    if (size < unitMaximum) {
      return `${(size / unitMultiplier).toFixed(1)} ${units[i]}`;
    }
  }
  return size;
}

export function triggerFileDownload(file: Blob, filename: string) {
  const link = document.createElement('a');
  link.href = window.URL.createObjectURL(file);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

export function cloneObject<T>(object: T): T {
  return JSON.parse(JSON.stringify(object));
}

export function shallowEqual(
  a: { [key: string]: unknown },
  b: { [key: string]: unknown }
) {
  if (a === b) {
    return true;
  }

  const aKeys = Object.keys(a);
  const bKeys = Object.keys(b);
  if (bKeys.length !== aKeys.length) {
    return false;
  }

  for (let i = 0; i < aKeys.length; i++) {
    const key = aKeys[i];
    if (a[key] !== b[key]) {
      return false;
    }
  }
  return true;
}
