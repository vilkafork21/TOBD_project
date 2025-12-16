# Руководство по работе с Git

## Начальная настройка Git

### 1. Установка Git (если еще не установлен)

```bash
# Проверка версии Git
git --version

# Если Git не установлен, установите через Homebrew
brew install git
```

### 2. Настройка имени и email

```bash
git config --global user.name "Ваше Имя"
git config --global user.email "your.email@example.com"
```

### 3. Проверка настроек

```bash
git config --list
```

## Работа с репозиторием

### Создание репозитория на GitHub

1. Зайдите на https://github.com
2. Нажмите кнопку "+" в правом верхнем углу
3. Выберите "New repository"
4. Введите имя репозитория (например: `energy-analytics`)
5. Выберите "Public" или "Private"
6. **НЕ** ставьте галочки на "Initialize with README"
7. Нажмите "Create repository"

### Связывание локального репозитория с GitHub

```bash
# Перейдите в директорию проекта
cd /Users/antonzyukov/Desktop/Tobd_project

# Проверьте статус Git (репозиторий уже инициализирован)
git status

# Добавьте удаленный репозиторий (замените USERNAME на ваш GitHub username)
git remote add origin https://github.com/USERNAME/energy-analytics.git

# Проверьте удаленные репозитории
git remote -v
```

## Основные команды Git

### Проверка статуса

```bash
# Просмотр измененных файлов
git status

# Просмотр изменений в файлах
git diff
```

### Добавление файлов

```bash
# Добавить все изменения
git add .

# Добавить конкретный файл
git add README.md

# Добавить все файлы в директории
git add spark_jobs/
```

### Создание коммита

```bash
# Создать коммит с сообщением
git commit -m "Описание изменений"

# Примеры хороших сообщений:
git commit -m "Initial commit: project structure"
git commit -m "Add ETL pipeline implementation"
git commit -m "Fix SQL queries in Streamlit dashboard"
```

### Отправка на GitHub

```bash
# Первая отправка (создание ветки main)
git push -u origin main

# Последующие отправки
git push
```

### Получение изменений с GitHub

```bash
# Получить изменения
git pull

# Если есть конфликты, Git покажет инструкции
```

## Типичный рабочий процесс

```bash
# 1. Проверить статус
git status

# 2. Добавить изменения
git add .

# 3. Создать коммит
git commit -m "Описание изменений"

# 4. Отправить на GitHub
git push
```

## Просмотр истории

```bash
# Краткая история коммитов
git log --oneline

# Подробная история
git log

# История с графиком веток
git log --graph --oneline --all
```

## Работа с ветками (опционально)

```bash
# Создать новую ветку
git branch feature-name

# Переключиться на ветку
git checkout feature-name

# Или создать и переключиться сразу
git checkout -b feature-name

# Вернуться на main
git checkout main

# Слить изменения из ветки в main
git merge feature-name

# Удалить ветку
git branch -d feature-name
```

## Игнорирование файлов

Файл `.gitignore` уже создан в проекте. Он исключает:
- Python кэш файлы (`__pycache__/`)
- Виртуальные окружения (`venv/`, `env/`)
- Большие CSV файлы (`data/raw/*.csv`)
- Логи Airflow
- IDE файлы

## Решение проблем

### Отмена изменений в файле

```bash
# Отменить изменения в файле (до добавления в staging)
git checkout -- filename

# Отменить добавление файла в staging
git reset HEAD filename
```

### Изменение последнего коммита

```bash
# Изменить сообщение последнего коммита
git commit --amend -m "Новое сообщение"

# Добавить файлы в последний коммит
git add forgotten_file.py
git commit --amend --no-edit
```

### Откат к предыдущему коммиту

```bash
# Просмотреть историю
git log --oneline

# Откатиться к конкретному коммиту (НЕ удаляя изменения)
git reset --soft <commit-hash>

# Откатиться и удалить изменения (ОСТОРОЖНО!)
git reset --hard <commit-hash>
```

## Полезные советы

1. **Делайте коммиты часто** - лучше много маленьких коммитов, чем один большой
2. **Пишите понятные сообщения** - опишите, что изменилось и зачем
3. **Проверяйте статус перед коммитом** - `git status` покажет, что будет закоммичено
4. **Не коммитьте большие файлы** - используйте `.gitignore` для CSV и других больших файлов
5. **Делайте pull перед push** - если работаете в команде, сначала получите изменения

## Полезные ссылки

- [Официальная документация Git](https://git-scm.com/doc)
- [GitHub Guides](https://guides.github.com/)
- [Интерактивный учебник Git](https://learngitbranching.js.org/)

