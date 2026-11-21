'use strict';

if (typeof Java.perform === 'undefined' && typeof Java.ready === 'function') {
  Java.perform = Java.ready;
}

Java.setStealth(2);
Java.perform(function () {
  console.log('[recomp-smoke] Java worker callback executed');
});
