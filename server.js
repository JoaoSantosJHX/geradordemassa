const express = require('express');
const parquet = require('parquetjs');
const { faker } = require('@faker-js/faker');
const path = require('path');
const fs = require('fs');

const app = express();
const port = 4000;

app.use(express.json());

app.get('/api/test', (req, res) => {
  res.json({ message: 'API funcionando!' });
});

// Endpoint para gerar o Parquet
app.post('/api/gerar-parquet', async (req, res) => {
  try {
    const { tableName, columns, numRows } = req.body;

    if (!tableName || !columns || !numRows) {
      return res.status(400).json({ error: 'Parâmetros inválidos.' });
    }

    // Montar schema Parquet dinamicamente
    const schemaDefinition = {};
    columns.forEach(col => {
      schemaDefinition[col.name] = { type: col.type };
    });

    const schema = new parquet.ParquetSchema(schemaDefinition);

    // Definir caminho do arquivo gerado
    const fileName = `${tableName}_${Date.now()}.parquet`;
    const filePath = path.join(__dirname, fileName);

    const writer = await parquet.ParquetWriter.openFile(schema, filePath);

    // Gerar dados aleatórios
    for (let i = 0; i < numRows; i++) {
      const row = {};
      for (const col of columns) {
        if (col.type === 'UTF8') {
          row[col.name] = faker.person.fullName(); // Nome aleatório
        } else if (col.type === 'DOUBLE') {
          row[col.name] = parseFloat(faker.finance.amount());
        } else if (col.type === 'INT64') {
          row[col.name] = faker.number.int({ min: 1, max: 9999 });
        } else {
          row[col.name] = null; // Valor padrão
        }
      }
      await writer.appendRow(row);
    }

    await writer.close();

    res.json({ message: 'Arquivo gerado com sucesso.', fileName: fileName });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Erro ao gerar o arquivo.' });
  }
});

// Endpoint para download
app.get('/api/download/:fileName', (req, res) => {
  const filePath = path.join(__dirname, req.params.fileName);
  if (fs.existsSync(filePath)) {
    res.download(filePath);
  } else {
    res.status(404).json({ error: 'Arquivo não encontrado.' });
  }
});

app.listen(port, () => {
  console.log(`Servidor backend rodando na porta ${port}`);
});
