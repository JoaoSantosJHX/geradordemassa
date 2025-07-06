const express = require('express');
const app = express();
const port = 4000;

app.use(express.json());

app.get('/api/test', (req, res) => {
  res.json({ message: 'API funcionando!' });
});

app.listen(port, () => {
  console.log(`Servidor backend rodando na porta ${port}`);
});
