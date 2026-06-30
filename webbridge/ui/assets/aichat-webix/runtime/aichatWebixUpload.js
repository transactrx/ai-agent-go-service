/* aichat-webix upload helper.
   One global: aichatWebixUpload = { uploadFile, fileToBase64 }.
   POSTs to the existing /aichatviewer/upload endpoint; reads file bytes as
   base64 for inline transport in the WS user_message frame. */

var aichatWebixUpload = (function () {
    async function uploadFile(file) {
        var form = new FormData();
        form.append("file", file, file.name);
        var resp = await fetch("aichatviewer/upload", { method: "POST", body: form, credentials: "include" });
        if (!resp.ok) {
            var body = "";
            try { body = await resp.text(); } catch (_) { /* ignore */ }
            throw new Error("upload failed (" + resp.status + "): " + (body || resp.statusText));
        }
        return resp.json();
    }

    // fileToBase64 reads the entire file and returns base64 with no MIME prefix.
    // Used to pack the bytes into the WS user_message frame so the agent can
    // feed them to Bedrock as native image/document content blocks.
    function fileToBase64(file) {
        return new Promise(function (resolve, reject) {
            var reader = new FileReader();
            reader.onload = function () {
                var result = String(reader.result || "");
                var comma = result.indexOf(",");
                resolve(comma >= 0 ? result.slice(comma + 1) : result);
            };
            reader.onerror = function () { reject(reader.error || new Error("FileReader failed")); };
            reader.readAsDataURL(file);
        });
    }

    return { uploadFile: uploadFile, fileToBase64: fileToBase64 };
})();
