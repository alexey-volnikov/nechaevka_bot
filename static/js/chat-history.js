// Модуль унифицированной истории чата для всех страниц приложения
(function (global) { // Оборачиваем код в самовызывающуюся функцию, чтобы не засорять глобальную область
  const placeholder = '<span class="text-secondary">—</span>'; // Плейсхолдер для пустых значений

  function buildAvatarLabel(label, avatarUrl) { // Строим подпись с аватаркой или только текстом
    const safeLabel = label || '—'; // Фолбэк на случай отсутствия подписи
    if (avatarUrl) { // Если есть ссылка на аватар
      return `<span class="d-inline-flex align-items-center gap-2"><img src="${avatarUrl}" alt="Аватар" class="avatar-inline" loading="lazy" /> <span>${safeLabel}</span></span>`; // Возвращаем HTML с картинкой и подписью
    } // Конец проверки аватара
    return safeLabel; // Возвращаем только текст, если аватара нет
  } // Конец функции сборки подписи с аватаром

  function buildPeerCell(message, options = {}) { // Строим ячейку чата с учетом настроек
    const chatName = message.peer_title || 'Чат без названия'; // Определяем название чата
    const avatarUrl = message.peer_avatar; // Читаем аватар чата
    const allowLink = options.allowLink !== false; // Разрешаем ссылку по умолчанию
    const hasId = Boolean(message.peer_id); // Проверяем наличие идентификатора чата
    if (allowLink && hasId) { // Если можно строить ссылку и есть ID
      return `<a href="/chat/${message.peer_id}" target="_blank" class="text-decoration-none text-light d-inline-flex align-items-center gap-2">${buildAvatarLabel(chatName, avatarUrl)}</a>`; // Возвращаем ссылку с аватаром и подписью
    } // Конец проверки возможности ссылки
    return buildAvatarLabel(chatName, avatarUrl); // Возвращаем подпись без ссылки
  } // Конец функции сборки ячейки чата

  function buildSenderCell(message, options = {}) { // Строим ячейку отправителя с плашкой бота
    const senderName = message.from_name || 'Неизвестный отправитель'; // Определяем имя отправителя
    const avatarUrl = message.from_avatar; // Читаем аватар отправителя
    const allowLink = options.allowLink !== false; // Разрешаем ссылку по умолчанию
    const showBotBadge = options.showBotBadge !== false; // Разрешаем бейдж бота по умолчанию
    const botBadge = message.is_bot && showBotBadge ? '<span class="sender-badge">Бот</span>' : ''; // Формируем бейдж бота при необходимости
    const baseLabel = buildAvatarLabel(senderName, avatarUrl); // Строим базовую подпись с аватаркой
    const hasId = Boolean(message.from_id); // Проверяем наличие ID отправителя
    if (allowLink && hasId) { // Если можно строить ссылку и есть ID
      return `<div class="d-flex align-items-center gap-2 flex-wrap"><a href="/user/${message.from_id}" target="_blank" class="text-decoration-none text-light d-inline-flex align-items-center gap-2">${baseLabel}</a>${botBadge}</div>`; // Возвращаем подпись со ссылкой на профиль
    } // Конец проверки возможности ссылки
    return `<div class="d-flex align-items-center gap-2 flex-wrap">${baseLabel}${botBadge}</div>`; // Возвращаем подпись без ссылки
  } // Конец функции сборки ячейки отправителя

  function buildReplyPreview(reply, options = {}) { // Собираем компактное или детальное превью ответа
    if (!reply) { // Если ответа нет
      return placeholder; // Возвращаем плейсхолдер
    } // Конец проверки ответа
    const mode = options.mode || 'compact'; // Режим отображения: compact или detailed
    if (mode === 'detailed') { // Ветка детализированного вида для админских таблиц
      if (!reply.id && !reply.text && !reply.from_id) { // Проверяем наличие полезных данных
        return placeholder; // Возвращаем плейсхолдер при пустых данных
      } // Конец проверки содержимого
      const replyId = reply.id ?? '—'; // Определяем ID исходного сообщения
      const replyText = reply.text ? reply.text.slice(0, 120) : 'Без текста'; // Готовим укороченный текст
      const replyAuthorLabel = reply.from_name ? `${reply.from_name} (ID: ${reply.from_id ?? '—'})` : reply.from_id ? `ID: ${reply.from_id}` : 'Автор неизвестен'; // Подпись автора
      const replyAuthorCell = buildAvatarLabel(replyAuthorLabel, reply.from_avatar); // Подпись автора с аватаром
      const peerId = options.peerId; // Берем peer_id для построения ссылки
      const replyLink = reply.id && peerId ? `<a href="https://vk.com/im?sel=${peerId}&msgid=${reply.id}" target="_blank" rel="noopener noreferrer">#${reply.id}</a>` : `#${replyId}`; // Строим ссылку на исходное сообщение
      const replyAttachmentsCount = Array.isArray(reply.attachments) ? reply.attachments.length : 0; // Считаем вложения в ответе
      const replyAttachmentsLabel = replyAttachmentsCount ? `${replyAttachmentsCount} влож.` : 'Без вложений'; // Формируем подпись количества вложений
      return `<div class="d-flex flex-column gap-1"><div class="d-flex align-items-center gap-2">${replyLink}<span class="text-secondary small">${replyAttachmentsLabel}</span></div><div class="text-secondary small">${replyText}</div><div class="text-secondary small">${replyAuthorCell}</div></div>`; // Возвращаем HTML блока ответа
    } // Конец ветки детального режима
    const hasReadableData = reply.text || reply.from_name; // Проверяем наличие человекочитаемых данных
    if (!hasReadableData) { // Если данных нет
      return placeholder; // Возвращаем плейсхолдер
    } // Конец проверки наличия данных
    const replyText = reply.text ? reply.text.slice(0, 120) : 'Без текста'; // Подготавливаем текст ответа
    const replyAuthorName = reply.from_name || 'Автор не указан'; // Определяем имя автора
    const replyBotBadge = reply.is_bot ? '<span class="sender-badge">Бот</span>' : ''; // Готовим бейдж бота, если нужно
    return `<div class="d-flex flex-column gap-1"><div class="text-secondary small">Ответ на предыдущее сообщение</div><div class="text-light small">${replyText}</div><div class="d-flex align-items-center gap-2 text-secondary small">${replyAuthorName}${replyBotBadge}</div></div>`; // Возвращаем компактный блок ответа
  } // Конец функции сборки превью ответа

  function prepareGalleryContent(message, galleryApi, galleryKey) { // Нормализуем вложения и регистрируем их в галерее
    const normalizedAttachments = galleryApi?.extractAttachments ? galleryApi.extractAttachments(message) : []; // Получаем нормализованные вложения
    const normalizedCopyHistory = galleryApi?.extractCopyHistory ? galleryApi.extractCopyHistory(message) : []; // Получаем нормализованные репосты
    if (galleryApi?.registerMessageGallery) { // Проверяем наличие функции регистрации галереи
      galleryApi.registerMessageGallery({ ...message, attachments: normalizedAttachments, copy_history: normalizedCopyHistory }, galleryKey); // Регистрируем вложения в общем модуле галереи
    } // Конец проверки регистрации
    const contentCell = galleryApi?.buildContentCell ? galleryApi.buildContentCell(normalizedAttachments, normalizedCopyHistory, galleryKey) : placeholder; // Формируем HTML ячейки контента
    return { contentCell, normalizedAttachments, normalizedCopyHistory }; // Возвращаем собранные данные для дальнейшего использования
  } // Конец функции подготовки вложений

  function buildDashboardRow(message, galleryApi, galleryKey, options = {}) { // Строим строку таблицы дашборда
    const row = document.createElement('tr'); // Создаем элемент строки
    if (options.isNew) { // Если строка новая
      row.classList.add('message-row-new'); // Добавляем класс анимации появления
    } // Конец проверки новизны строки
    if (message.is_deleted) { // Если сообщение помечено как удаленное
      row.classList.add('message-deleted'); // Подсвечиваем строку красноватым фоном
    } // Конец проверки удаления
    const formatDate = options.formatDate || ((value) => value || '—'); // Берем функцию форматирования времени или плейсхолдер
    const createdAt = formatDate(message.created_at); // Форматируем время создания записи
    const peerCell = buildPeerCell(message, { allowLink: true }); // Строим ячейку чата с ссылкой
    const authorCell = buildSenderCell(message, { allowLink: true, showBotBadge: true }); // Строим ячейку отправителя с плашкой бота
    const replyCell = buildReplyPreview(message.reply, { mode: 'compact' }); // Строим компактное превью ответа
    const { contentCell } = prepareGalleryContent(message, galleryApi, galleryKey); // Готовим вложения и галерею
    const textCell = message.text ?? ''; // Берем текст сообщения или пустую строку
    row.innerHTML = `<td class="text-secondary small text-nowrap">${createdAt}</td><td class="text-nowrap">${peerCell}</td><td class="text-nowrap">${authorCell}</td><td>${replyCell}</td><td>${contentCell}</td><td>${textCell}</td>`; // Заполняем HTML строки с дополнительной колонкой времени и запретом переноса в ключевых столбцах
    return row; // Возвращаем готовую строку
  } // Конец функции построения строки дашборда

  function buildLogsRow(log, galleryApi, galleryKey, options = {}) { // Строим строку таблицы логов
    const row = document.createElement('tr'); // Создаем элемент строки
    if (log.is_deleted) { // Если лог отмечен как удаленный
      row.classList.add('message-deleted'); // Добавляем класс подсветки удаления
    } // Конец проверки удаления
    const formatDate = options.formatDate || ((value) => value || '—'); // Берем функцию форматирования времени
    const createdAt = formatDate(log.created_at); // Форматируем время создания
    const peerCell = buildPeerCell(log, { allowLink: true }); // Строим ячейку чата
    const authorCell = buildSenderCell(log, { allowLink: true, showBotBadge: true }); // Строим ячейку отправителя
    const botBadge = log.is_bot ? 'Да' : 'Нет'; // Определяем флаг бота
    const replyCell = buildReplyPreview(log.reply, { mode: 'detailed', peerId: log.peer_id }); // Строим детальное превью ответа
    const { contentCell } = prepareGalleryContent(log, galleryApi, galleryKey); // Готовим вложения
    const textCell = log.text ?? ''; // Берем текст или пустую строку
    const messageIdCell = log.message_id ?? '—'; // Берем ID сообщения
    const logIdCell = log.id ?? '—'; // Берем ID записи лога
    const deleteButton = `<button type="button" class="btn btn-sm btn-outline-danger delete-log-btn" data-log-id="${log.id}">Удалить</button>`; // Формируем кнопку удаления
    row.innerHTML = `<td class="text-nowrap">${createdAt}</td><td class="text-nowrap">${peerCell}</td><td class="text-nowrap">${authorCell}</td><td class="text-nowrap">${botBadge}</td><td>${replyCell}</td><td>${contentCell}</td><td>${textCell}</td><td class="text-nowrap">${messageIdCell}</td><td class="text-nowrap">${logIdCell}</td><td>${deleteButton}</td>`; // Заполняем HTML строки таблицы логов с запретом переноса ключевых ячеек
    return row; // Возвращаем готовую строку
  } // Конец функции построения строки логов

  function buildEntityRow(message, galleryApi, galleryKey, options = {}) { // Строим строку для страниц профилей чатов и пользователей
    const row = document.createElement('tr'); // Создаем строку таблицы
    if (message.is_deleted) { // Если сообщение помечено как удаленное
      row.classList.add('message-deleted'); // Подсвечиваем строку в таблице профиля
    } // Конец проверки удаления
    const formatDate = options.formatDate || ((value) => value || '—'); // Берем функцию форматирования времени
    const showChatColumn = Boolean(options.showChatColumn); // Определяем, нужно ли показывать столбец чата
    const showSenderColumn = Boolean(options.showSenderColumn); // Определяем, нужно ли показывать столбец отправителя
    const createdAtCell = formatDate(message.created_at); // Форматируем время создания сообщения
    const peerCell = buildPeerCell(message, { allowLink: true }); // Собираем ячейку чата
    const authorCell = buildSenderCell(message, { allowLink: true, showBotBadge: true }); // Собираем ячейку отправителя
    const { contentCell } = prepareGalleryContent(message, galleryApi, galleryKey); // Подготавливаем вложения и создаем ячейку с бейджами
    const textCell = message.text ?? '—'; // Берем текст сообщения или плейсхолдер
    const cells = [`<td class="text-nowrap">${createdAtCell}</td>`]; // Начинаем набор ячеек с времени без переноса
    if (showChatColumn) { // Если нужно показывать столбец чата
      cells.push(`<td class="text-nowrap">${peerCell}</td>`); // Добавляем ячейку чата без переноса
    } // Конец проверки столбца чата
    if (showSenderColumn) { // Если нужно показывать столбец отправителя
      cells.push(`<td class="text-nowrap">${authorCell}</td>`); // Добавляем ячейку отправителя без переноса
    } // Конец проверки столбца отправителя
    cells.push(`<td>${contentCell}</td>`); // Добавляем ячейку с вложениями
    cells.push(`<td>${textCell}</td>`); // Добавляем ячейку с текстом
    row.innerHTML = cells.join(''); // Склеиваем все ячейки в строку
    return row; // Возвращаем готовую строку
  } // Конец функции построения строки профиля

  function registerEntityGalleries(messages, galleryApi) { // Регистрируем галереи на страницах профиля чата/пользователя
    if (!Array.isArray(messages)) { // Проверяем, что данные корректные
      return; // Прерываемся, если нет массива
    } // Конец проверки формата
    messages.forEach((msg, idx) => { // Перебираем каждое сообщение
      const galleryKey = `entity-msg-${idx}`; // Формируем ключ галереи по индексу
      prepareGalleryContent(msg, galleryApi, galleryKey); // Регистрируем вложения в модуле галереи
    }); // Конец перебора сообщений
  } // Конец функции регистрации галерей профиля

  global.chatHistory = { // Экспортируем публичное API
    buildAvatarLabel, // Выносим сборку подписи с аватаром
    buildPeerCell, // Выносим сборку ячейки чата
    buildSenderCell, // Выносим сборку ячейки отправителя
    buildReplyPreview, // Выносим превью ответа
    prepareGalleryContent, // Выносим подготовку вложений и регистрацию галерей
    buildDashboardRow, // Выносим построение строки дашборда
    buildLogsRow, // Выносим построение строки логов
    buildEntityRow, // Выносим построение строки профиля чата или пользователя
    registerEntityGalleries, // Выносим регистрацию галерей в профилях
  }; // Завершаем экспорт
})(window); // Передаем window как глобальный объект
