document.getElementById('upload-form').addEventListener('submit', async function(event) {
  event.preventDefault();
  const fileInput = document.getElementById('file-input');
  const file = fileInput.files[0];
  const uploadButton = document.getElementById('upload-button');

  if (!file) {
      document.getElementById('status').innerText = "ファイルを選択してください。";
      return;
  }

  const formData = new FormData();
  formData.append('file', file);

  // Add loading spinner
  const spinner = document.createElement('span');
  spinner.className = 'spinner';
  uploadButton.appendChild(spinner);
  uploadButton.disabled = true;

  try {
      const response = await fetch('/process_xlsx/', {
          method: 'POST',
          body: formData
      });

      if (response.ok) {
          const blob = await response.blob();
          const downloadUrl = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = downloadUrl;
          a.download = '非対面状況元データ.xlsx';
          document.body.appendChild(a);
          a.click();
          a.remove();
          URL.revokeObjectURL(downloadUrl);
          document.getElementById('status').innerText = "ファイルが正常に処理されました。";
      } else {
          const errorText = await response.text();
          document.getElementById('status').innerText = `エラーが発生しました: ${errorText}`;
      }
  } catch (error) {
      document.getElementById('status').innerText = `エラーが発生しました: ${error.message}`;
  } finally {
      // Remove loading spinner
      spinner.remove();
      uploadButton.disabled = false;
  }
});

document.getElementById('file-input').addEventListener('change', function() {
  const fileInput = document.getElementById('file-input');
  const file = fileInput.files[0];
  const fileNameDisplay = document.getElementById('file-name');
  const uploadButton = document.getElementById('upload-button');

  if (file) {
      fileNameDisplay.innerText = `選択されたファイル: ${file.name}`;
      uploadButton.disabled = false; // ファイルが選択されたらボタンを有効化
  } else {
      fileNameDisplay.innerText = "選択されていません";
      uploadButton.disabled = true; // ファイルが選択されていない場合ボタンを無効化
  }
});

// 初期状態でアップロードボタンを無効化
document.getElementById('upload-button').disabled = true;
